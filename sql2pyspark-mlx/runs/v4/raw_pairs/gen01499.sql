with with_dims as (
    -- Attach orders attributes
    select
        s.*,
        o.amount as order_amount,
        o.channel
    from payments s
    left join orders o
        on s.order_id = o.id
)
-- Grouped metrics
select
    date_trunc('year', paid_at) as year_start,
    count(distinct method) as distinct_methods,
    percentile_approx(amount, 0.95) as amount_p95,
    sum(case when channel = 'partner' then order_amount else 0 end) as partner_order_amount
from with_dims
group by year_start
having distinct_methods < 1
