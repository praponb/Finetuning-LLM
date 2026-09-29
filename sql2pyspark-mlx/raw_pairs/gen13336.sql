SELECT s.city, SUM(p.price) AS total_price, MIN(p.stock_qty) AS min_stock_qty FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id GROUP BY s.city
