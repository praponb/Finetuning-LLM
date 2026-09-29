select id, order_id from order_items where product_id = 8 intersect select id, order_id from order_items where unit_price < 180;
