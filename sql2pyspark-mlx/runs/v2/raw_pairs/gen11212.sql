select id, store_id, order_date, amount, lead(amount, 1, 0) over (partition by store_id order by order_date, id) as next_amount from orders;
