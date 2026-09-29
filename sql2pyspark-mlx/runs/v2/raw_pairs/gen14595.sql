select id, order_date, store_id, shipping_fee, amount + discount as amount_plus_discount from orders where shipping_fee <= 13 or amount < 540 order by id desc limit 5
