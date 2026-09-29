select category, round(stddev(price), 2) as stddev_price
from products
where category in ('Home', 'Fashion', 'Sports') and stock_qty <= 280 and array_contains(tags, 'eco')
group by category
order by stddev_price;
