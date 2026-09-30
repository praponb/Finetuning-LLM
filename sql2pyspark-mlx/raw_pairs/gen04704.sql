select id, quantity, product_id from order_items where quantity is null intersect select id, quantity, product_id from order_items where quantity = 7 order by product_id;
