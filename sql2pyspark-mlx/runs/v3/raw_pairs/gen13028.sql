SELECT last_restock, 'inventory' source
FROM inventory
UNION
SELECT sale_date last_restock, 'store_sale' source
FROM store_sales
WHERE units IS NOT NULL
ORDER BY source DESC;
