SELECT store_sales.id, store_sales.store_id FROM store_sales WHERE store_sales.store_id IN (SELECT id FROM stores WHERE YEAR(opened_date) >= 2024) ORDER BY store_sales.id, store_id
