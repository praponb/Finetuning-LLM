SELECT id, channel, store_id
FROM orders
WHERE status <> 'RETURNED'
EXCEPT
SELECT id, channel, store_id
FROM orders
WHERE amount IS NOT NULL;
