SELECT store_id, MAX(units) - MIN(units) AS units_range, ROUND(AVG(revenue), 2) AS avg_revenue
FROM store_sales
WHERE store_id IN (3, 5)
GROUP BY store_id
ORDER BY avg_revenue DESC, 1 DESC;
