select id, order_id, amount, method from (select *, row_number() over (partition by order_id order by amount desc, id) as rn from payments where paid_at < '2023-09-01') as ranked where rn <= 3
