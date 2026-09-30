with recent_shipments as (
    select *
    from shipments
    where shipped_date > '2023-08-21' and month(delivered_date) in (4, 6, 10)
),
DeliveredYearSummary as (
    select year(delivered_date) as delivered_year, round(stddev(cost), 2) as stddev_cost, min(cost) as min_cost
    from recent_shipments
    group by year(delivered_date)
)
select delivered_year, stddev_cost
from DeliveredYearSummary
