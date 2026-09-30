select p.supplier_id, p.category, p.launch_date, oi.product_id, oi.quantity from products p left outer join order_items oi on oi.product_id = p.id order by p.supplier_id desc;
