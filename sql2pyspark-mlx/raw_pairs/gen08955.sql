SELECT
    o.id,
    o.store_id,
    o.channel,
    o.amount,
    s.city
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
ORDER BY 2 DESC, id ASC
LIMIT 10
