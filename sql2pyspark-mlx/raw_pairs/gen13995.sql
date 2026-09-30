SELECT
    QUARTER(launch_date) AS launch_quarter,
    DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month,
    category,
    ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty,
    MAX(stock_qty) AS max_stock_qty
FROM products
WHERE NOT (ARRAY_CONTAINS(tags, 'new') AND brand IN ('Globex', 'Acme'))
GROUP BY 1, 2, 3
