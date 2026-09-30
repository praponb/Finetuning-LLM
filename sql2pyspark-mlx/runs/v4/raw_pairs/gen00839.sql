select
    month(order_date) as order_month,
    sum(discount) as total_discount,
    count(*) as cnt,
    round(sum(discount) * 100.0 / sum(sum(discount)) over (), 2) as pct_of_total
from orders
group by month(order_date)
order by total_discount desc, 4 desc;
