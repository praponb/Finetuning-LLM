with ranked as (select *, rank() over (partition by order_id order by paid_at) as rnk from payments) select id, order_id, paid_at, method from ranked where rnk <= 1;
