SELECT order_items.id, order_items.unit_price, order_items.product_id, products.stock_qty FROM order_items INNER JOIN products ON order_items.product_id = products.id
