SELECT
    channel,
    store_id,
    CASE WHEN shipping_fee >= 40 THEN 'high' WHEN shipping_fee >= 12 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket,
    COUNT(shipping_fee) AS shipping_fee_count
FROM orders
WHERE status = 'RETURNED'
GROUP BY 1, 2, 3
HAVING COUNT(shipping_fee) >= 5
ORDER BY store_id DESC;
