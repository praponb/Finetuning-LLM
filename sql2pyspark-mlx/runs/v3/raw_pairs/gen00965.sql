SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, CASE WHEN brand IN ('Umbrella', 'Globex') THEN 'group_a' ELSE 'other' END AS brand_group, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty
FROM products
GROUP BY 1, 2;
