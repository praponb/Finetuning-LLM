with valid_payments as (
    select *
    from payments
    where amount > 1480
),
payments_stats as (
    select
        order_id,
        quarter(paid_at) as paid_quarter,
        sort_array(collect_set(method)) as method_list,
        min(paid_at) as min_paid_at
    from valid_payments
    group by order_id, quarter(paid_at)
)
select *
from payments_stats
order by min_paid_at asc;
