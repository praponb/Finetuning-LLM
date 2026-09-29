SELECT p.id, p.stock_qty FROM products p WHERE p.id IN (SELECT product_id FROM inventory WHERE last_restock <= '2023-04-08') ORDER BY id DESC LIMIT 10
