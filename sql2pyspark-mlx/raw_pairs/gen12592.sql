SELECT last_restock, 'inventory' record_type
FROM inventory
WHERE last_restock IS NOT NULL
UNION
SELECT opened_date, 'store' record_type
FROM stores
WHERE opened_date IS NOT NULL
