select
    date_trunc('MONTH', launch_date) as launch_month,
    sum(stock_qty) as total_stock_qty,
    count(*) as cnt,
    round(sum(stock_qty) * 100.0 / sum(sum(stock_qty)) over (), 2) as pct_of_total
from products
group by date_trunc('MONTH', launch_date)
order by total_stock_qty desc;
