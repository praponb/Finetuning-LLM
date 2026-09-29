select id, method, paid_at, order_id from (select *, dense_rank() over (partition by method order by paid_at desc) as rnk from payments) as r where rnk <= 1
