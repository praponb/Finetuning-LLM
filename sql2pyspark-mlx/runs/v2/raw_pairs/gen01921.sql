SELECT p.id, p.cost, p.stock_qty FROM products p WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.units BETWEEN 18 AND 26)
