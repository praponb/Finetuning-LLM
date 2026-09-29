SELECT
    channel,
    CASE WHEN discount >= 0.32 THEN 'high' WHEN discount >= 0.17 THEN 'mid' ELSE 'low' END AS discount_bucket,
    MAX(amount) AS max_amount,
    AVG(discount) AS avg_discount,
    COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_count
FROM orders
GROUP BY 1, 2;
