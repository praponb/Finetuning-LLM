with shipments_by_order as (
    select order_id, max(cost) - min(cost) as cost_range, round(stddev(cost), 2) as stddev_cost
    from shipments
    group by order_id
)
select
    o.id,
    o.customer_id,
    o.order_date,
    sbo.cost_range,
    sbo.stddev_cost
from orders o
inner join shipments_by_order sbo on o.id = sbo.order_id;
