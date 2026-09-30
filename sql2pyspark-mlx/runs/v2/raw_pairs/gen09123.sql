with top_payments as (select *, rank() over (partition by order_id order by amount) as rn from payments) select id, order_id, amount from top_payments where rn <= 2 order by id desc
