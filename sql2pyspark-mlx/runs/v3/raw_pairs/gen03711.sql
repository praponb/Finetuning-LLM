select
    oi.id,
    oi.unit_price,
    p.category,
    p.product_name,
    o.customer_id,
    o.discount
from order_items oi
inner join products p on oi.product_id = p.id
inner join orders o on oi.order_id = o.id
where o.amount between 770 and 1530 or oi.unit_price < 290
order by o.discount desc;
