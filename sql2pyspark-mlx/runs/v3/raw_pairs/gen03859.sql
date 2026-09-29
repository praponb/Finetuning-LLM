select
    category,
    brand,
    min(launch_date) min_launch_date,
    sum(price) total_price
from products
where product_name is null and stock_qty between 266 and 381
group by category, brand
having sum(price) > 6300
order by min_launch_date asc;
