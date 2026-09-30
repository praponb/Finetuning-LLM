SELECT QUARTER(sale_date) AS sale_quarter, SUM(units) AS total_units, SUM(revenue) AS total_revenue
FROM store_sales
GROUP BY QUARTER(sale_date)
HAVING SUM(units) >= 140;
