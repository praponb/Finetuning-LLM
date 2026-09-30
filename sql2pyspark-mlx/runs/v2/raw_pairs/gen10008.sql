SELECT revenue, sale_date
FROM store_sales
WHERE store_id IN (2, 3, 4, 6)
ORDER BY revenue
