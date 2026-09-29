select id, status, order_date, amount, amount - lag(amount, 1, 0) over (partition by status order by order_date, id) as amount_change from orders;
