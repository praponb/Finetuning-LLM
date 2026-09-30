SELECT product_id, COUNT(revenue) AS revenue_count
FROM store_sales
GROUP BY product_id
