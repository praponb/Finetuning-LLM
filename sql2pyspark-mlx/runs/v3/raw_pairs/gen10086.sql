SELECT product_id, QUARTER(sale_date) AS sale_quarter, ROUND(AVG(units), 2) AS avg_units
FROM store_sales
GROUP BY product_id, QUARTER(sale_date)
