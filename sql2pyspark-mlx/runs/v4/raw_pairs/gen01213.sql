select category, max_stock_qty
from (
    select category, max(stock_qty) as max_stock_qty
    from products
    group by category
) as t
where max_stock_qty >= 210
order by category;
