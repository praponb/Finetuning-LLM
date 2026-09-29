select * from (select id, order_id, paid_at, row_number() over (partition by order_id order by paid_at, id) as pos from payments) as t where pos <= 3
