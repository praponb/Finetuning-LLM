select customer_id, order_date from orders where store_id is null intersect select customer_id, order_date from orders where shipping_fee is not null;
