select count(*) as product_count, round(stddev(price), 2) as stddev_price from products
