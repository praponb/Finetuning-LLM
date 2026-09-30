select supplier_id, round(stddev(price), 2) as stddev_price, round(stddev(stock_qty), 2) as stddev_stock_qty from products where brand = 'Globex' group by supplier_id order by stddev_price desc
