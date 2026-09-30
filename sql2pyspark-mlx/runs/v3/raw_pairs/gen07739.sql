with base as (
    select *
    from shipments
    where delivered_date <= '2024-04-20' and order_id = 15 and status <> 'IN_TRANSIT'
),
shipments_stats as (
    select status, max(cost) - min(cost) as cost_range, count(distinct cost) as unique_carriers
    from base
    group by status
)
select status, cost_range
from shipments_stats
where cost_range < 52
order by status desc
