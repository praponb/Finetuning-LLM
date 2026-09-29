with cleaned as (
    select
        id,
        order_id,
        method,
        date_trunc('year', paid_at) as paid_at_period
    from payments
    where paid_at >= timestamp '2023-10-07 20:00:00'
      and method is not null
)
select
    date_trunc('week', paid_at_period) as week_start,
    (unix_timestamp(max(paid_at_period)) - unix_timestamp(min(paid_at_period))) / 60 as span_minutes,
    count(*) as cnt,
    size(collect_set(method)) as num_methods,
    count_if(method = 'cash') as cash_events,
    count_if(method = 'cash') as cash_events_2
from cleaned
group by week_start
order by cash_events_2;
