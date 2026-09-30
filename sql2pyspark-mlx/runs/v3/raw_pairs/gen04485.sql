SELECT store_id, MAX(sale_date) AS latest_sale_date, ROUND(AVG(units), 2) AS avg_units
FROM store_sales
WHERE units < 23
GROUP BY store_id
HAVING ROUND(AVG(units), 2) >= 71
ORDER BY avg_units DESC
