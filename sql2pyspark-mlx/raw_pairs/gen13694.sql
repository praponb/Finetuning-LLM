select count(cost) as cost_count, min(stock_qty) as min_stock_qty, round(stddev(price), 2) as stddev_price
from products;
