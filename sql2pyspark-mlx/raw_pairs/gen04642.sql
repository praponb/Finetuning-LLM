with product_totals as (
    select product_id, max(unit_price) - min(unit_price) unit_price_range
    from order_items
    group by product_id
)
select
    p.id,
    p.product_name,
    p.brand,
    pt.unit_price_range
from products p
inner join product_totals pt on p.id = pt.product_id
where pt.unit_price_range >= 58
order by unit_price_range
