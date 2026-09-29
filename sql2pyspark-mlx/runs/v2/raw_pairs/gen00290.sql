select o.amount, t.total_quantity
from orders o
left join (select order_id, sum(quantity) total_quantity
    from order_items
    where order_id in (1, 4, 30, 33)
    group by order_id) t on o.id = t.order_id
order by total_quantity desc;
