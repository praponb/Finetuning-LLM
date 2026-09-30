select customer_id, channel, order_date from orders where (status = 'CANCELLED' and shipping_fee >= 30) or store_id = 2 order by order_date asc;
