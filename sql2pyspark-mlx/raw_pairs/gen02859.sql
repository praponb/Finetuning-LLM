select id, status from (select *, rank() over (partition by order_id, method order by paid_at desc nulls first) as latest_rank from payments) x where latest_rank = 1;
