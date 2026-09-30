SELECT id, last_restock, product_id, COALESCE(quantity, 0) AS quantity_filled, ROUND(quantity, 1) AS quantity_rounded FROM inventory WHERE last_restock BETWEEN '2024-10-01' AND '2024-12-01'
