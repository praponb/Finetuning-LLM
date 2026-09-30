SELECT id, city, segment, CONCAT_WS('-', segment, city) AS segment_city_key FROM customers WHERE segment = 'Corporate' AND is_active
