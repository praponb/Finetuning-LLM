SELECT
    status,
    store_id,
    customer_id,
    ROUND(STDDEV(discount), 2) AS stddev_discount,
    COUNT(DISTINCT discount) AS unique_statuss
FROM orders
WHERE channel IN ('web', 'partner')
GROUP BY status, store_id, customer_id
