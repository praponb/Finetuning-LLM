select
    o.amount,
    o.discount,
    t.revenue,
    t.row_count
from orders o
inner join (select order_id, sum(unit_price * quantity) as revenue, count(*) as row_count
    from order_items
    group by order_id) t on o.id = t.order_id
where t.revenue > 71
order by o.discount desc, t.revenue desc;
