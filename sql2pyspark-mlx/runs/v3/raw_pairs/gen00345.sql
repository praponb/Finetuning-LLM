SELECT *
FROM (
    SELECT category, CASE WHEN cost >= 550 THEN 'high' WHEN cost >= 190 THEN 'mid' ELSE 'low' END AS cost_bucket, AVG(cost) AS avg_cost
    FROM products
    GROUP BY category, CASE WHEN cost >= 550 THEN 'high' WHEN cost >= 190 THEN 'mid' ELSE 'low' END
) AS agg
WHERE avg_cost > 350
