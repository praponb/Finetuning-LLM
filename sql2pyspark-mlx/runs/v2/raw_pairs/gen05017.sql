SELECT i.store_id, s.opened_date, s.region, CONCAT_WS(',', s.store_name, s.city) AS store_name_city_key FROM inventory i LEFT JOIN stores s ON i.store_id = s.id WHERE s.sqft BETWEEN 11287 AND 12884;
