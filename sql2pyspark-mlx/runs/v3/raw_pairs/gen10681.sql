select p.stock_qty, oi.quantity from products p left join order_items oi on oi.product_id = p.id where p.price <= 820;
