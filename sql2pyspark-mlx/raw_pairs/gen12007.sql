SELECT id, sqft, city, ROW_NUMBER() OVER (ORDER BY sqft DESC, id) AS sqft_row, COUNT(*) OVER (PARTITION BY city) AS city_count FROM stores ORDER BY city;
