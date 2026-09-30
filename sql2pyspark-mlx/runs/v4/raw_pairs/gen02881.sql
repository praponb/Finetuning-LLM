with order_items_by_product as (
    select product_id, max(unit_price) as max_unit_price, count(unit_price) as unit_price_count
    from order_items
    group by product_id
)
select
    p.cost,
    p.launch_date,
    oibp.max_unit_price,
    oibp.unit_price_count
from products p
inner join order_items_by_product oibp on p.id = oibp.product_id
where oibp.max_unit_price >= 150
order by p.cost desc
