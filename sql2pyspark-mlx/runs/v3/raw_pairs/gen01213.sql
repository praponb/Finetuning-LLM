select id, order_date, shipping_fee, sum(shipping_fee) over (order by order_date, id) running_shipping_fee from orders
