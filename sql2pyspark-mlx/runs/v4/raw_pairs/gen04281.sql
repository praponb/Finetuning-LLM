SELECT *
FROM (
    SELECT
        CASE WHEN discount >= 0.43 THEN 'high' WHEN discount >= 0.2 THEN 'mid' ELSE 'low' END AS discount_bucket,
        status,
        AVG(amount) AS avg_amount,
        COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count
    FROM orders
    WHERE customer_id IS NULL
    GROUP BY CASE WHEN discount >= 0.43 THEN 'high' WHEN discount >= 0.2 THEN 'mid' ELSE 'low' END, status
) AS s
WHERE store_count < 9
ORDER BY avg_amount DESC
