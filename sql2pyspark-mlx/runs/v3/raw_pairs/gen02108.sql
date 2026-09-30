SELECT store_id, MAX(revenue) AS max_revenue, COUNT(units) AS units_count
FROM store_sales
GROUP BY store_id
