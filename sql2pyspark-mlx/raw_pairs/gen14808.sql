SELECT a.id, a.product_id, a.sale_date, b.cost, b.product_name FROM store_sales a INNER JOIN products b ON a.product_id = b.id WHERE b.launch_date BETWEEN '2024-06-01' AND '2025-04-01'
