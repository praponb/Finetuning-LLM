SELECT oi.id, oi.order_id, p.cost FROM order_items oi INNER JOIN products p ON oi.product_id = p.id WHERE SIZE(p.tags) > 1
