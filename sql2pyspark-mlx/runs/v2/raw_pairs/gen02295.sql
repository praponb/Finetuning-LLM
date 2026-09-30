SELECT QUARTER(sale_date) AS sale_quarter, SUM(units) AS total_units, SUM(SUM(units)) OVER (ORDER BY QUARTER(sale_date)) AS cumulative_units
FROM store_sales
WHERE sale_date >= '2025-07-01'
GROUP BY QUARTER(sale_date)
