SELECT product_id, units, revenue FROM store_sales WHERE NOT (product_id IN (2, 3, 9) AND sale_date BETWEEN '2023-03-01' AND '2024-09-16') ORDER BY revenue;
