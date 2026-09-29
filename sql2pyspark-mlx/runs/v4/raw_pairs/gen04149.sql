with recent_subscriptions as (
    select *, ceil(monthly_fee) as monthly_fee_whole
    from subscriptions
    where plan in ('premium', 'enterprise', 'standard') and customer_id in (21, 27)
),
agg as (
    select status, sort_array(collect_set(status)) as status_list, max(monthly_fee_whole) - min(monthly_fee_whole) as monthly_fee_whole_range
    from recent_subscriptions
    group by status
)
select status, status_list, monthly_fee_whole_range
from agg
where monthly_fee_whole_range >= 57
order by status;
