select
    p.id,
    p.launch_date,
    t.revenue,
    t.avg_quantity
from products p
inner join (select product_id, sum(quantity * unit_price) as revenue, avg(quantity) as avg_quantity
    from order_items
    group by product_id) t on p.id = t.product_id
where t.revenue >= 27;
