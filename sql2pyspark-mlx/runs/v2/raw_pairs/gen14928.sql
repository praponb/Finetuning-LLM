select status, round(stddev(shipping_fee), 2) stddev_shipping_fee from orders where channel in ('partner', 'mobile') and channel = 'store' group by status
