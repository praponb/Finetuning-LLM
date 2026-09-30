SELECT
    CASE WHEN discount >= 0.36 THEN 'high' WHEN discount >= 0.09 THEN 'mid' ELSE 'low' END AS discount_bucket,
    SUM(discount) AS total_discount,
    COUNT(*) AS cnt,
    ROUND(SUM(discount) * 100.0 / SUM(SUM(discount)) OVER (), 2) AS pct_of_total
FROM orders
WHERE order_date <= '2023-06-01'
GROUP BY CASE WHEN discount >= 0.36 THEN 'high' WHEN discount >= 0.09 THEN 'mid' ELSE 'low' END
