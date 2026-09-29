SELECT *
FROM (
    SELECT CASE WHEN amount >= 1310 THEN 'high' WHEN amount >= 780 THEN 'mid' ELSE 'low' END AS amount_bucket, COUNT(discount) AS discount_count, SUM(discount) AS total_discount
    FROM orders
    WHERE customer_id IN (5, 30, 40)
    GROUP BY CASE WHEN amount >= 1310 THEN 'high' WHEN amount >= 780 THEN 'mid' ELSE 'low' END
) AS t
WHERE total_discount < 1.9;
