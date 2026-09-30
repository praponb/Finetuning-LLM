select oi.unit_price, oi.order_id, o.shipping_fee, p.cost, p.price from order_items oi join orders o on oi.order_id = o.id join products p on oi.product_id = p.id;
