SELECT sale_date, 'store_sale' record_type
FROM store_sales
WHERE sale_date IS NOT NULL
UNION
SELECT last_restock, 'inventory' record_type
FROM inventory
WHERE last_restock IS NOT NULL
