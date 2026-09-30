select id, channel, status, order_date, amount, lag(amount, 2) over (partition by channel, status order by order_date, id) as prev_amount from orders where customer_id = 30 order by channel asc;
