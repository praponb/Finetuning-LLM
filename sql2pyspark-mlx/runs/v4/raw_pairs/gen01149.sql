with base as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(method, 'unassigned') as method_filled,
        coalesce(method, 'N/A') as resolved_method
    from payments
    where amount between 1140 and 1390
)
-- Multi-level aggregation
select
    date_trunc('week', paid_at) as week_start,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as span_minutes,
    size(collect_set(method)) as num_methods,
    max(paid_at) as last_seen,
    sum(case when status = 'REFUNDED' then amount else 0 end) as refunded_amount,
    count_if(status = 'REFUNDED') as refunded_events,
    avg(amount) as avg_amount
from base
group by date_trunc('week', paid_at)
having size(collect_set(method)) < 1
