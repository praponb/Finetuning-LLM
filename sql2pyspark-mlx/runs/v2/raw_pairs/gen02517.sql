SELECT store_name FROM stores WHERE opened_date > '2023-04-19' UNION ALL SELECT store_name FROM stores WHERE opened_date >= '2024-11-01' ORDER BY store_name
