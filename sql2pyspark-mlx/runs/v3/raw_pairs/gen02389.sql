SELECT
    o.id,
    o.discount,
    o.channel,
    o.status
FROM orders o
WHERE o.store_id IN (SELECT id FROM stores WHERE sqft < 10335)
