select order_items.id, order_items.order_id, order_items.quantity, order_items.product_id, products.launch_date from order_items left outer join products on order_items.product_id = products.id;
