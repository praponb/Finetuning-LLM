with Numbered as (select *, row_number() over (partition by order_id order by paid_at desc, id) row_num from payments) select id, order_id, paid_at, method from Numbered where row_num = 1
