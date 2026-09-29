select p.product_name, oi.quantity from products p left outer join order_items oi on oi.product_id = p.id;
