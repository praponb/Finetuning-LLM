SELECT p.id, p.cost, p.stock_qty FROM products p WHERE p.launch_date BETWEEN '2024-01-01' AND '2025-06-01' AND p.id IN (SELECT product_id FROM store_sales WHERE product_id = 10);
