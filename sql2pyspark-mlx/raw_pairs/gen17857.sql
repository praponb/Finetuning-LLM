SELECT store_id, COUNT(*) AS cnt, MAX(revenue) AS max_revenue
FROM store_sales
WHERE (store_id IN (1, 4, 5, 6) AND revenue IS NULL) OR revenue >= 1100
GROUP BY store_id
