with order_summary as (
    select order_id, count(distinct unit_price) as unique_products
    from order_items
    where product_id is not null
    group by order_id
)
select o.amount, o.channel, os.unique_products
from orders o
left join order_summary os on o.id = os.order_id
where os.unique_products >= 7
order by amount, unique_products desc;
