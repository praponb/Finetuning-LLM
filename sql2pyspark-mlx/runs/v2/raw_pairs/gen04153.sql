select id, status, paid_at, order_id from (select id, status, paid_at, order_id, row_number() over (partition by status order by paid_at desc, id) as rn from payments) as t where rn <= 3;
