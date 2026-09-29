with OrderStats as (
    select order_id, sum(case when carrier = 'Flash' then cost else 0 end) as flash_cost
    from shipments
    group by order_id
)
select
    o.id,
    o.amount,
    o.store_id,
    O.flash_cost
from orders o
left join OrderStats O on o.id = O.order_id
where O.flash_cost >= 64
order by flash_cost desc;
