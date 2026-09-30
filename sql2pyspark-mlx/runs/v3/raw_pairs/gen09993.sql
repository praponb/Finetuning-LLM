select *
from (
    select
        brand,
        month(launch_date) as launch_month,
        max(cost) as max_cost,
        avg(stock_qty) as avg_stock_qty
    from products
    where array_contains(tags, 'new')
    group by brand, month(launch_date)
) as t
where avg_stock_qty > 280
order by avg_stock_qty desc, launch_month desc;
