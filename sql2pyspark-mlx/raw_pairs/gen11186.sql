select
    o.id,
    o.status,
    t.pct_flash,
    t.lost_cost
from orders o
join (select order_id, round((100 * sum(case when carrier = 'Flash' then 1 else 0 end)) / count(*), 1) as pct_flash, sum(case when status = 'LOST' then cost else 0 end) as lost_cost
    from shipments
    where delivered_date <= '2024-11-01'
    group by order_id) t on o.id = t.order_id
