select id, order_date, shipping_fee, lead(shipping_fee, 1) over (order by order_date, id) as next_shipping_fee from orders;
