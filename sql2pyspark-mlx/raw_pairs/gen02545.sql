select oi.id, oi.order_id, oi.unit_price, oi.quantity, p.supplier_id, p.brand, p.price from order_items oi inner join products p on oi.product_id = p.id where oi.unit_price < 200;
