select category, round(avg(price), 2) as avg_price, round(stddev(stock_qty), 2) as stddev_stock_qty
from products
where product_name like '%Yog%'
group by category
