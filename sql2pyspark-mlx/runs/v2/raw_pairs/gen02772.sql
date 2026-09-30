SELECT
    brand,
    QUARTER(launch_date) AS launch_quarter,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty,
    MIN(launch_date) AS first_launch_date
FROM products
GROUP BY brand, launch_quarter
ORDER BY 3 ASC, brand DESC, launch_quarter ASC
LIMIT 1;
