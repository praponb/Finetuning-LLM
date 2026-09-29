SELECT
    id,
    category,
    cost,
    ROUND(cost / SUM(cost) OVER (PARTITION BY category), 3) AS cost_share
FROM products;
