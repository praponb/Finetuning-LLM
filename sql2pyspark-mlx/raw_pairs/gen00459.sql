WITH filtered_orders AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), order_date) / 365.25 AS years_since_order_date
    FROM orders
    WHERE customer_id IN (4, 16, 20, 29)
),
agg AS (
    SELECT CASE WHEN discount >= 0.43 THEN 'high' WHEN discount >= 0.16 THEN 'mid' ELSE 'low' END AS discount_bucket, ROUND(AVG(years_since_order_date), 2) AS avg_years_since_order_date
    FROM filtered_orders
    GROUP BY CASE WHEN discount >= 0.43 THEN 'high' WHEN discount >= 0.16 THEN 'mid' ELSE 'low' END
)
SELECT discount_bucket, avg_years_since_order_date
FROM agg
WHERE avg_years_since_order_date < 67;
