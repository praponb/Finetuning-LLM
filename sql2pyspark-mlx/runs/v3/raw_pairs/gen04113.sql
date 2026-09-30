select p.id, p.brand, p.launch_date, p.price from products p where not exists (select 1 from order_items oi where oi.product_id = p.id) order by brand
