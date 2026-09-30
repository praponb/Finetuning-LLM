select oi.unit_price, o.shipping_fee, p.brand, p.launch_date from order_items oi inner join orders o on oi.order_id = o.id inner join products p on oi.product_id = p.id order by p.brand;
