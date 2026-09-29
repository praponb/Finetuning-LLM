select o.id, o.store_id, t.refunded_count
from orders o
inner join (select order_id, sum(case when status = 'REFUNDED' then 1 else 0 end) as refunded_count
    from payments
    group by order_id) t on o.id = t.order_id
order by o.store_id desc, id
limit 5;
