with ranked as (select *, row_number() over (partition by order_id order by paid_at desc, id) as pos from payments where order_id = 34) select id, order_id, paid_at from ranked where pos <= 5;
