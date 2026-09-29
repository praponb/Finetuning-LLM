select
    year(order_date) as order_year,
    round(stddev(shipping_fee), 2) as stddev_shipping_fee,
    round(stddev(amount), 2) as stddev_amount,
    max(amount) - min(amount) as amount_range
from orders
group by 1
order by amount_range desc
