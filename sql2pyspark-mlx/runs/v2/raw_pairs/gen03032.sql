select o.customer_id, o.channel, t.in_transit_count
from orders o
join (select order_id, count(case when status = 'IN_TRANSIT' then 1 end) as in_transit_count
    from shipments
    group by order_id) t on o.id = t.order_id
where t.in_transit_count > 8
order by channel desc;
