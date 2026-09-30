with ProductTotals as (
    select product_id, round(stddev(unit_price), 2) stddev_unit_price, max(unit_price) max_unit_price
    from order_items
    where quantity > 7
    group by product_id
)
select
    products.id,
    products.supplier_id,
    ProductTotals.stddev_unit_price,
    ProductTotals.max_unit_price
from products
left outer join ProductTotals on products.id = ProductTotals.product_id
order by products.id desc
