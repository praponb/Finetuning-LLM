select
    o.id,
    o.customer_id,
    o.discount,
    t.unique_products
from orders o
left join (select order_id, count(distinct unit_price) as unique_products
    from order_items
    group by order_id) t on o.id = t.order_id
where t.unique_products >= 5
order by id desc;
