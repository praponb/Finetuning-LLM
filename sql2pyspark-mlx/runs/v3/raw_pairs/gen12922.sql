SELECT
    CASE WHEN stores.sqft >= 15414 THEN 'high' WHEN stores.sqft >= 7568 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    stores.city,
    COUNT(CASE WHEN payments.status = 'REFUNDED' THEN 1 END) AS refunded_count,
    COUNT(*) AS total_count
FROM payments
LEFT OUTER JOIN orders ON payments.order_id = orders.id
LEFT OUTER JOIN stores ON orders.store_id = stores.id
GROUP BY CASE WHEN stores.sqft >= 15414 THEN 'high' WHEN stores.sqft >= 7568 THEN 'mid' ELSE 'low' END, stores.city
ORDER BY sqft_bucket;
