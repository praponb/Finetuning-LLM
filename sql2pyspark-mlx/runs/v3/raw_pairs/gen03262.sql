select o.store_id, o.discount, t.cnt
from orders o
left join (select order_id, count(*) as cnt
    from shipments
    where order_id is not null
    group by order_id) t on o.id = t.order_id;
