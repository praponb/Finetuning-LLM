SELECT inventory.store_id, store_sales.revenue, store_sales.units
FROM inventory
JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE inventory.quantity BETWEEN 139 AND 152 AND inventory.last_restock BETWEEN '2023-05-01' AND '2024-01-01'
ORDER BY revenue DESC, store_sales.units DESC;
