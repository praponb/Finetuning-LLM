SELECT id, opened_date, store_name FROM stores WHERE opened_date > '2023-08-01' INTERSECT SELECT id, opened_date, store_name FROM stores WHERE store_name NOT LIKE 'M%' ORDER BY store_name DESC
