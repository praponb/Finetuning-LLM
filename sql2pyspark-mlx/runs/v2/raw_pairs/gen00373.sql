SELECT *
FROM orders
WHERE store_id IS NOT NULL AND channel <> 'web' AND discount BETWEEN 0.18 AND 0.27
