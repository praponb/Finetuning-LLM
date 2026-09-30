SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, CASE WHEN brand IN ('Initech', 'Acme') THEN 'tier_1' ELSE 'other' END AS brand_group, ROUND(AVG(stock_qty), 2) AS avg_stock_qty
FROM products
WHERE supplier_id = 37
GROUP BY ROLLUP(DATE_FORMAT(launch_date, 'yyyy-MM'), CASE WHEN brand IN ('Initech', 'Acme') THEN 'tier_1' ELSE 'other' END)
ORDER BY launch_month
