with shipments_by_order as (
    select order_id, count(case when status = 'IN_TRANSIT' then 1 end) in_transit_count, min(shipped_date) first_shipped_date
    from shipments
    group by order_id
)
select
    o.id,
    o.store_id,
    sbo.in_transit_count,
    sbo.first_shipped_date
from orders o
inner join shipments_by_order sbo on o.id = sbo.order_id
order by first_shipped_date
