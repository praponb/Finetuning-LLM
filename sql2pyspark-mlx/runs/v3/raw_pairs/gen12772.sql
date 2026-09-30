SELECT QUARTER(launch_date) AS launch_quarter, YEAR(launch_date) AS launch_year, SUM(CASE WHEN category = 'Electronics' THEN cost ELSE 0 END) AS electronics_cost
FROM products
WHERE stock_qty IS NOT NULL AND launch_date <= '2025-06-22'
GROUP BY launch_quarter, launch_year
ORDER BY launch_year DESC, electronics_cost DESC;
