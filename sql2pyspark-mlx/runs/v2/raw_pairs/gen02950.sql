SELECT
    o.channel,
    o.order_date,
    o.discount,
    s.city,
    s.region
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE o.customer_id IS NOT NULL
ORDER BY order_date DESC
