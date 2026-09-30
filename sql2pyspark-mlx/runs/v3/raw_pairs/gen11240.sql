select round(stddev(price), 2) as stddev_price, count(stock_qty) as stock_qty_count from products where brand in ('Stark', 'Umbrella', 'Initech')
