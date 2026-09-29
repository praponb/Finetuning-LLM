with recent_shipments as (
    select *
    from shipments
    where month(shipped_date) in (3, 5, 10)
),
agg as (
    select status, round(avg(cost), 2) as avg_cost
    from recent_shipments
    group by status
)
select status, avg_cost
from agg
where avg_cost >= 79
order by status desc;
