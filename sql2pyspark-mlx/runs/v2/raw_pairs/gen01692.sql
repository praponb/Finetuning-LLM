SELECT
    store_id,
    status,
    COUNT(shipping_fee) AS shipping_fee_count,
    ROUND((100 * SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_store,
    SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END) AS store_count
FROM orders
WHERE (status = 'COMPLETED' AND order_date >= '2023-04-01') OR shipping_fee BETWEEN 12 AND 24
GROUP BY store_id, status
HAVING SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END) >= 67
