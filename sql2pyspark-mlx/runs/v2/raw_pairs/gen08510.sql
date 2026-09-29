select
    id,
    store_id,
    sale_date,
    revenue,
    units,
    round(avg(revenue) over (partition by store_id order by sale_date, id rows between 6 preceding and current row), 2) as revenue_moving_avg_7,
    percent_rank() over (partition by store_id order by units desc) as units_pct_rank
from store_sales
where store_id in (1, 2, 3, 4);
