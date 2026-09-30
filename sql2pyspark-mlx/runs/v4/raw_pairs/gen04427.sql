with valid as (
    select
        *,
        case when status in ('FAILED', 'REFUNDED') then 'Group A' else 'Other' end as status_group,
        coalesce(method, 'DIRECT') as method_filled
    from payments
    where status = 'FAILED'
)
select
    method,
    round(avg(amount), 2) as avg_amount,
    avg(amount) filter (where method = 'paypal') as paypal_avg_amount,
    count(distinct id) as distinct_ids,
    min(paid_at) as first_seen,
    max(paid_at) as ended_at,
    max(paid_at) as last_seen
from valid
group by method
order by distinct_ids nulls first
