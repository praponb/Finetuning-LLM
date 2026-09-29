with valid as (
    select
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        cast(cost as int) cost_int
    from shipments
    where delivered_date > '2023-07-18'
)
select
    o.status as order_status,
    s.status as shipment_status,
    count(*) as cnt,
    max(s.cost_int) as max_cost_int,
    round(avg(s.cost), 2) as avg_cost,
    grouping_id() as level_id
from valid s
left join orders o on s.order_id = o.id
group by grouping sets ((o.status, s.status), (o.status), ())
order by avg_cost desc nulls last;
