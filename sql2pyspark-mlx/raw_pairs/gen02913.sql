SELECT
    QUARTER(launch_date) AS launch_quarter,
    category,
    CASE WHEN brand IN ('Acme', 'Globex') THEN 'priority' ELSE 'other' END AS brand_group,
    MAX(stock_qty) AS max_stock_qty
FROM products
WHERE ARRAY_CONTAINS(tags, 'sale') AND category = 'Books'
GROUP BY QUARTER(launch_date), category, CASE WHEN brand IN ('Acme', 'Globex') THEN 'priority' ELSE 'other' END
ORDER BY category;
