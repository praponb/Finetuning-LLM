SELECT sale_date FROM store_sales WHERE store_id IN (2, 4) UNION ALL SELECT last_restock FROM inventory ORDER BY sale_date
