SELECT city, CONCAT_WS('-', segment, name) AS segment_name_key, COALESCE(city, 'N/A') AS city_filled
FROM customers;
