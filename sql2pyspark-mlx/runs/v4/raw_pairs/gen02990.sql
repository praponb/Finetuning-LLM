with shipments_by_order as (
    select order_id, count(distinct cost) as unique_statuss
    from shipments
    group by order_id
)
select o.id, o.channel, sbo.unique_statuss
from orders o
left outer join shipments_by_order sbo on o.id = sbo.order_id
where sbo.unique_statuss >= 4
order by 3
