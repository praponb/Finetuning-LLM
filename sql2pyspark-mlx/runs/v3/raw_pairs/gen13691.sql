SELECT p.price, p.cost, p.brand, oi.unit_price, oi.quantity FROM products p INNER JOIN order_items oi ON oi.product_id = p.id ORDER BY price;
