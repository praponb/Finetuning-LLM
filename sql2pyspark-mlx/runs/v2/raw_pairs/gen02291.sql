SELECT product_id, MIN(revenue) AS min_revenue, MAX(sale_date) AS last_sale_date
FROM store_sales
GROUP BY product_id
HAVING MIN(revenue) >= 3700
