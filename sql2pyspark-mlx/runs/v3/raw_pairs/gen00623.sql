SELECT p.id, p.price, t.avg_quantity FROM products p INNER JOIN (SELECT product_id, AVG(quantity) avg_quantity FROM inventory GROUP BY product_id) t ON p.id = t.product_id WHERE t.avg_quantity > 160;
