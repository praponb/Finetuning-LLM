select id, order_id, amount, min(amount) over (partition by order_id) as order_id_min_amount from payments order by id desc
