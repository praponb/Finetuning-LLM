SELECT *
FROM (
    SELECT
        category,
        CASE WHEN brand IN ('Initech', 'Stark') THEN 'group_a' ELSE 'other' END AS brand_group,
        SUM(stock_qty) AS total_stock_qty,
        MAX(cost) - MIN(cost) AS cost_range
    FROM products
    WHERE SIZE(tags) >= 2
    GROUP BY category, CASE WHEN brand IN ('Initech', 'Stark') THEN 'group_a' ELSE 'other' END
) AS x
WHERE cost_range > 22
ORDER BY cost_range DESC
