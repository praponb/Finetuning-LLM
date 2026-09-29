select id, order_id, paid_at from (select *, row_number() over (partition by order_id order by paid_at, id) as rn from payments) as t where rn <= 2 order by 2 asc, id desc;
