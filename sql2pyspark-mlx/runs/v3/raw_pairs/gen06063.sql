SELECT last_restock event_date
FROM inventory
WHERE store_id IN (1, 3, 4, 5)
UNION ALL
SELECT opened_date event_date
FROM stores
WHERE store_name IS NULL;
