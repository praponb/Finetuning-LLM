with product_stats as (
    select product_id, sum(units) as total_units
    from store_sales
    group by product_id
)
select p.stock_qty, p.price, ps.total_units
from products p
left outer join product_stats ps on p.id = ps.product_id
order by price desc;
