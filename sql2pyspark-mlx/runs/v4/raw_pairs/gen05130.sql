with product_summary as (
    select product_id, min(units) as min_units
    from store_sales
    where year(sale_date) = 2025
    group by product_id
)
select p.stock_qty, p.supplier_id, ps.min_units
from products p
inner join product_summary ps on p.id = ps.product_id
order by ps.min_units;
