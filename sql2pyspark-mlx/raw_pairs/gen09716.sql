select
    id,
    product_name,
    cost,
    stock_qty,
    launch_date,
    avg(stock_qty) over (partition by supplier_id, brand order by price, id rows between 6 preceding and 1 preceding) as stock_qty_avg_6_prior,
    avg(price) over (order by stock_qty, id rows between 3 preceding and 1 preceding) as price_avg_3_prior,
    percent_rank() over (partition by year(launch_date) order by price nulls last) as price_percent_rank,
    avg(cost) over w_ordered as cost_avg_6,
    ntile(4) over w_ordered_2 as cost_bucket
from products
window w_ordered as (partition by year(launch_date), category order by launch_date, id rows between 6 preceding and current row), w_ordered_2 as (order by cost desc, id);
