package com.univest.app.brokering.service;

import com.univest.app.brokering.dto.LocationCategory;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
@Service
public class DataAPIScraper {

    static final String API_URL = "https://blinkit.com/v1/layout/listing_widgets";
    static final String COOKIE = "__cf_bm=tBjkT2H1l1e5Ky8hRjbedol8VoUD2gxzOkotE1azo18-1744358045-1.0.1.1-7FDM6ZOoWxO1YHAOCzmPIZOnFgIrKEAR5HbtSEaRfFIYHciaPjFPi0ps9D2bQW.rDNOkffunb1F2l3eAJeBZZ6HSXlpYj_G9JrLxbH5xVdg; __cfruid=56eb355d6bcc237022df35aeb4c14a6ae1be1e5c-1744358045; _cfuvid=u3LsaX59j7o6j1_VBectEh7AYCxdI0ABCKTNaWA7wnM-1744358045864-0.0.1.1-604800000";
    static final OkHttpClient client = new OkHttpClient();

    public void runScripts() {
        List<LocationCategory> list = new ArrayList<>();
        String[][] categories = {
                {"Munchies", "1237", "Bhujia & Mixtures", "1178"},
                {"Munchies", "1237", "Munchies Gift Packs", "1694"},
                {"Munchies", "1237", "Namkeen Snacks", "29"},
                {"Munchies", "1237", "Papad & Fryums", "80"},
                {"Munchies", "1237", "Chips & Crisps", "940"},
                {"Sweet Tooth", "9", "Indian Sweets", "943"}
        };

        String[][] locations = {
                {"28.678051", "77.314262"},
                {"28.5045", "77.012"},
                {"22.59643333", "88.39996667"},
                {"23.1090018", "72.57299832"},
                {"18.95833333", "72.83333333"},
                {"28.7045", "77.15366667"},
                {"12.88326667", "77.5594"},
                {"28.7295", "77.12866667"},
                {"28.67571622", "77.36149677"},
                {"28.3501", "77.31673333"},
                {"28.59086667", "77.3054"},
                {"28.49553604", "77.51297417"},
                {"28.44176667", "77.3084"},
                {"28.48783333", "77.09533333"},
                {"12.93326667", "77.61773333"},
                {"13.00826667", "77.64273333"},
                {"28.4751", "77.4334"},
                {"26.85653333", "75.71283333"},
                {"26.8982", "75.8295"},
                {"18.54316667", "73.914"}
        };

        for (String[] cat : categories) {
            String l0_cat = cat[1];
            String l1_cat = cat[3];
            for (String[] loc : locations) {
                String lat = loc[0];
                String lon = loc[1];
                list.add(new LocationCategory(l0_cat, l1_cat, lat, lon));
            }
        }
        runScrapesForAll(list);
    }

    public JSONObject runScrapeForSingle(String l0_cat, String l1_cat, String lat, String lon) {
        try {
            log.info("Starting BlinkIt scrape for l0_cat: {}, l1_cat: {}, lat: {}, lon: {}", l0_cat, l1_cat, lat, lon);
            JSONObject postBody = createPostBody();
            return makePostRequest(postBody, lat, lon, l0_cat, l1_cat);
        } catch (Exception e) {
            log.error("Error while scraping BlinkIt data for l1_cat: {}: {}", l1_cat, e.getMessage(), e);
            return new JSONObject();
        }
    }

    public void runScrapesForAll(List<LocationCategory> entries) {
        String directoryPath = "/Users/Univest/Downloads/blink_it_CSVs";
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = sdf.format(new Date());
        String fileName = directoryPath + "/blinkit_all_data_" + timestamp + ".csv";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write("l0_category,l1_category,lat,lon,variant_id,variant_name,selling_price,mrp,in_stock,inventory,is_sponsored,image_url,brand_id,brand");
            writer.newLine();
            for (LocationCategory entry : entries) {
                JSONObject response = runScrapeForSingle(entry.getL0_cat(), entry.getL1_cat(), entry.getLat(), entry.getLon());
                extractAndSave(response, entry.getL0_cat(), entry.getL1_cat(), entry.getLat(), entry.getLon(), writer);
            }
            writer.flush();
            log.info("All data written to CSV file: {}", fileName);
        } catch (Exception e) {
            log.error("Error in runScrapesForAll: {}", e.getMessage(), e);
        }
    }

    private JSONObject createPostBody() {
        JSONObject body = new JSONObject();
        body.put("applied_filters", JSONObject.NULL);
        body.put("is_sr_rail_visible", false);
        body.put("is_subsequent_page", false);
        body.put("shown_product_count", 15);
        body.put("sort", "");

        JSONObject meta = new JSONObject();
        meta.put("primary_results_group_ids", new JSONArray(Arrays.asList(
                1912612,
                616421,
                804280,
                727789,
                727790,
                850767,
                727792,
                727793,
                727791,
                1917211,
                447220,
                1910164,
                524408,
                1910235,
                1911071
        )));
        meta.put("primary_results_product_ids", new JSONArray(Arrays.asList(
                381071,
                482066,
                527251,
                527261,
                527262,
                527263,
                527264,
                499231,
                373410,
                481827,
                373412,
                481826,
                527265,
                535079,
                535080,
                539822,
                432818,
                432819,
                11060,
                210876,
                563004,
                518846,
                518847,
                21572,
                521414,
                521415,
                521416,
                521417,
                521418,
                591,
                592,
                544721,
                381269,
                525142,
                522838,
                525144,
                525145,
                525143,
                525146,
                531030,
                537182,
                557662,
                56803,
                531562,
                572143,
                566911
        )));
        body.put("postback_meta", meta);

        JSONObject rails = new JSONObject();
        String[] keys = {"aspirational_card_rail", "attribute_rail", "brand_rail", "dc_rail", "priority_dc_rail"};
        for (String key : keys) {
            JSONObject r = new JSONObject();
            r.put("total_count", 0);
            r.put("processed_count", 1);
            r.put("processed_product_ids", new JSONArray());
            rails.put(key, r);
        }
        body.put("processed_rails", rails);

        return body;
    }

    private JSONObject makePostRequest(JSONObject postData, String lat, String lon, String l0_cat, String l1_cat) {
        try {
            String fullUrl = API_URL
                    + "?offset=0"
                    + "&limit=100"
                    + "&exclude_combos=false"
                    + "&l0_cat=" + l0_cat
                    + "&l1_cat=" + l1_cat
                    + "&last_snippet_type=product_card_snippet_type_2"
                    + "&last_widget_type=product_container"
                    + "&oos_visibility=true"
                    + "&page_index=1"
                    + "&total_entities_processed=1"
                    + "&total_pagination_items=44";

            RequestBody requestBody = RequestBody.create(
                    postData.toString(), MediaType.parse("application/json"));

            Request request = new Request.Builder()
                    .url(fullUrl)
                    .addHeader("lat", lat)
                    .addHeader("lon", lon)
                    .addHeader("Content-Type", "application/json")
                    .addHeader("Cookie", COOKIE)
                    .addHeader("Accept", "application/json, text/plain, */*")
                    .addHeader("Accept-Language", "en-US,en;q=0.9")
                    .addHeader("Origin", "https://blinkit.com")
                    .addHeader("Referer", "https://blinkit.com/")
                    .addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
                    .post(requestBody)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    log.error("Non-200 response code: {}", response.code());
                    return new JSONObject();
                }
                String responseBody = response.body().string();
                return new JSONObject(responseBody);
            }
        } catch (Exception e) {
            log.error("Exception in makePostRequest: {}", e.getMessage(), e);
            return new JSONObject();
        }
    }

    private void extractAndSave(JSONObject json, String l0_cat, String l1_cat, String lat, String lon, BufferedWriter writer) {
        try {
            JSONObject response = json.optJSONObject("response");
            if (response == null) {
                log.warn("No response found in JSON for l1_cat: {}", l1_cat);
                return;
            }
            JSONArray snippets = response.optJSONArray("snippets");
            if (snippets == null) {
                log.warn("No snippets found in response for l1_cat: {}", l1_cat);
                return;
            }
            for (int i = 0; i < snippets.length(); i++) {
                JSONObject snippet = snippets.getJSONObject(i);
                JSONObject data = snippet.optJSONObject("data");
                if (data == null) {
                    continue;
                }
                JSONObject identity = data.optJSONObject("identity");
                String variant_id = identity != null ? safe(identity.optString("id")) : "";
                JSONObject nameObj = data.optJSONObject("name");
                String variant_name = nameObj != null ? safe(nameObj.optString("text")).replaceAll(",", "") : "";
                JSONObject normalPriceObj = data.optJSONObject("normal_price");
                String selling_price = normalPriceObj != null ? safe(normalPriceObj.optString("text")) : "";
                JSONObject mrpObj = data.optJSONObject("mrp");
                String mrp = mrpObj != null ? safe(mrpObj.optString("text")) : "";
                int inv = data.optInt("inventory", 0);
                String inventory = String.valueOf(inv);
                String in_stock = inv > 0 ? "true" : "false";
                String is_sponsored = "";
                JSONObject imageObj = data.optJSONObject("image");
                String image_url = imageObj != null ? safe(imageObj.optString("url")) : "";
                String brand_id = "";
                JSONObject brandNameObj = data.optJSONObject("brand_name");
                String brand = brandNameObj != null ? safe(brandNameObj.optString("text")) : "";
                String row = String.join(",",
                        l0_cat,
                        l1_cat,
                        lat,
                        lon,
                        variant_id,
                        variant_name,
                        selling_price,
                        mrp,
                        in_stock,
                        inventory,
                        is_sponsored,
                        image_url,
                        brand_id,
                        brand
                );
                writer.write(row);
                writer.newLine();
            }
        } catch (Exception e) {
            log.error("Error saving data for l1_cat: {}: {}", l1_cat, e.getMessage(), e);
        }
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

}
