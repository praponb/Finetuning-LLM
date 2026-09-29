SELECT city, region FROM stores WHERE store_name IS NOT NULL UNION ALL SELECT city, region FROM stores WHERE opened_date >= '2024-06-12' ORDER BY city
