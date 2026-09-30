select oi.id, oi.product_id, oi.order_id, p.stock_qty, p.brand, o.shipping_fee from order_items oi join products p on oi.product_id = p.id right join orders o on oi.order_id = o.id;
