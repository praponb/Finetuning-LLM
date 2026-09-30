SELECT
    CASE WHEN category IN ('Books', 'Sports') THEN 'core' ELSE 'other' END AS category_group,
    SUM(price) AS total_price,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(price) DESC) AS price_rank
FROM products
GROUP BY CASE WHEN category IN ('Books', 'Sports') THEN 'core' ELSE 'other' END
