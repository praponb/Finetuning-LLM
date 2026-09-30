SELECT store_id, units, revenue
FROM store_sales
WHERE store_id = 4 AND sale_date BETWEEN '2024-04-01' AND '2024-11-01' AND product_id IN (1, 4, 7, 8)
ORDER BY revenue DESC;
