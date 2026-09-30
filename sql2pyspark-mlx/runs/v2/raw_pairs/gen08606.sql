SELECT product_id, DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, MAX(units) AS max_units
FROM store_sales
WHERE units = 37 AND revenue > 1800 AND product_id IN (2, 5)
GROUP BY product_id, DATE_FORMAT(sale_date, 'yyyy-MM')
ORDER BY product_id DESC;
