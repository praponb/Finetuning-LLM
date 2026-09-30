select status, date_format(order_date, 'yyyy-MM') as order_month, round(stddev(amount), 2) as stddev_amount from orders where channel is null group by cube(status, date_format(order_date, 'yyyy-MM'))
