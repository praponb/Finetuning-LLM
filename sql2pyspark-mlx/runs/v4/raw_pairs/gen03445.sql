select
    date_trunc('quarter', launch_date) as quarter_start,
    date_trunc('month', launch_date) as activity_month,
    approx_percentile(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
    count(case when category = 'Books' then 1 end) as books_count,
    dense_rank() over (partition by date_trunc('quarter', launch_date) order by count(case when category = 'Books' then 1 end) desc) as books_count_dense_rank
from products
group by date_trunc('quarter', launch_date), date_trunc('month', launch_date)
order by activity_month desc
