select order_date, customer_id from orders where amount < 570 union select order_date, customer_id from orders where store_id in (3, 39);
