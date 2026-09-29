with SupplierStats as (
    select supplier_id, max(launch_date) as last_launch_date, sum(stock_qty) as total_stock_qty
    from products
    where product_name like '%Mat%'
    group by supplier_id
)
select
    s.id,
    s.rating,
    s.city,
    S.last_launch_date,
    S.total_stock_qty
from suppliers s
inner join SupplierStats S on s.id = S.supplier_id
where S.total_stock_qty > 1800
order by total_stock_qty, rating desc
