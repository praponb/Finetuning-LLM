with filtered_shipments as (
    select *
    from shipments
    where delivered_date > '2025-07-01'
),
shipments_stats as (
    select status, count(cost) as cost_count
    from filtered_shipments
    group by status
)
select *
from shipments_stats;
