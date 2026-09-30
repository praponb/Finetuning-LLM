SELECT * FROM store_sales WHERE product_id IN (2, 5, 6, 10) AND (sale_date BETWEEN '2023-04-01' AND '2024-08-01' OR revenue BETWEEN 2100 AND 4000)
