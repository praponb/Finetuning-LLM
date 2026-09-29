SELECT city FROM stores WHERE store_name LIKE '%Riversid%' UNION ALL SELECT city FROM stores WHERE YEAR(opened_date) = 2023;
