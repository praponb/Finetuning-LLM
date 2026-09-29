select id, channel from orders where month(order_date) in (1, 3, 12) and discount < 0.36;
