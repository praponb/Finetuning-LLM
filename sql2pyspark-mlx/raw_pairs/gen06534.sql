select
    o.id,
    o.discount,
    o.store_id,
    t.status_list
from orders o
inner join (select order_id, sort_array(collect_set(status)) status_list
    from payments
    group by order_id) t on o.id = t.order_id
order by id desc
