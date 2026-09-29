SELECT id, store_id FROM store_sales WHERE revenue < 2300 AND product_id IN (1, 8, 9, 10) AND sale_date >= '2025-07-10' ORDER BY store_id DESC;
