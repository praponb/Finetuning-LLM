SELECT id, city, opened_date, sqft FROM stores WHERE opened_date > '2024-06-08' AND (YEAR(opened_date) = 2023 OR LENGTH(store_name) > 11);
