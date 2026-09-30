SELECT store_name FROM stores WHERE city <> 'Singapore' INTERSECT SELECT store_name FROM stores WHERE MONTH(opened_date) IN (1, 5, 9);
