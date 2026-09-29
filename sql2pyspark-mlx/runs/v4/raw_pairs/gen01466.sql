select id, customer_id, order_date from orders where store_id is not null intersect select id, customer_id, order_date from orders where order_date > '2023-07-01';
