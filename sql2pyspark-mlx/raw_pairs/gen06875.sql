SELECT category_group, avg_cost
FROM (
    SELECT CASE WHEN category IN ('Sports', 'Books') THEN 'priority' ELSE 'other' END AS category_group, AVG(cost) AS avg_cost
    FROM products
    GROUP BY CASE WHEN category IN ('Sports', 'Books') THEN 'priority' ELSE 'other' END
) AS agg
WHERE avg_cost >= 370
ORDER BY category_group
LIMIT 10;
