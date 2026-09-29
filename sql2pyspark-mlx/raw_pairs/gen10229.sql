with cleaned as (
    select
        id,
        carrier,
        shipped_date,
        status,
        datediff(current_date(), shipped_date) as days_since_shipped_date,
        date_trunc('month', delivered_date) as delivered_date_period
    from shipments
    where shipped_date >= date '2023-08-23'
      and carrier is distinct from 'Kerry'
)
select *
from (
    select
        carrier,
        status,
        days_since_shipped_date
    from cleaned
) src
pivot (
    sum(days_since_shipped_date) as value_sum,
    count(1) as cnt
    for status in ('DELIVERED', 'IN_TRANSIT')
);
