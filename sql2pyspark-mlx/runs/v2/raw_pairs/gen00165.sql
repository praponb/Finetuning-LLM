SELECT city, DATE_TRUNC('week', opened_date) AS opened_date_period, CONCAT_WS('/', city, region) AS city_region_key FROM stores ORDER BY city_region_key DESC;
