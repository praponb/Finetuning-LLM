SELECT oi.id, oi.product_id, oi.quantity, oi.order_id, p.price, p.cost, p.price * p.cost margin FROM order_items oi FULL OUTER JOIN products p ON oi.product_id = p.id WHERE SIZE(p.tags) > 2
