SELECT 'store_sale' AS kind, sale_date FROM store_sales WHERE units < 40 UNION ALL SELECT 'inventory' AS kind, last_restock FROM inventory ORDER BY kind DESC;
