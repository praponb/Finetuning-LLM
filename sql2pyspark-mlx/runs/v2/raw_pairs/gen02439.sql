select p.price, p.brand, oi.quantity, oi.product_id, oi.unit_price, oi.unit_price * oi.quantity as line_total from products p left outer join order_items oi on p.id = oi.product_id
