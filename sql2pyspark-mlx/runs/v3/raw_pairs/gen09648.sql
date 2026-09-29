select id, status, channel, order_date, amount, lead(amount, 1) over (partition by status, channel order by order_date, id) as next_amount from orders where discount <= 0.26
