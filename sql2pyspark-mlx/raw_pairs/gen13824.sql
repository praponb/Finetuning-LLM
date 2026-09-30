select order_items.id, order_items.quantity from order_items where order_items.order_id in (9, 17) and order_items.product_id in (select id from products where supplier_id is null);
