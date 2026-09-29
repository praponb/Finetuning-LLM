with src as (
    select *
    from shipments
    where shipped_date <= '2025-05-24'
),
summary as (
    select carrier, round(avg(cost), 2) as avg_cost, count(distinct cost) as unique_orders
    from src
    group by carrier
)
select *
from summary;
