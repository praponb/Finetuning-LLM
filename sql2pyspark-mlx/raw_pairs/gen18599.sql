select
    p.channel,
    p.status as order_status,
    e.status as shipment_status,
    count(*) as rows_total,
    max(e.cost) as max_cost
from shipments e
left join orders p on e.order_id = p.id
group by p.channel, p.status, e.status;
