SELECT s.city, ROUND(STDDEV(p.price), 2) AS stddev_price, AVG(p.stock_qty) AS avg_stock_qty FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id GROUP BY s.city;
