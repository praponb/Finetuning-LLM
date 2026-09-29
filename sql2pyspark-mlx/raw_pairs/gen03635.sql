SELECT id, product_id, quantity, SUM(quantity) OVER (PARTITION BY product_id) AS product_id_sum_quantity FROM inventory WHERE last_restock BETWEEN '2024-07-01' AND '2024-10-01'
