select
    id,
    delivered_date,
    cost,
    order_id,
    shipped_date,
    round(avg(cost) over (order by delivered_date, id rows between 2 preceding and current row), 2) as cost_moving_avg_3,
    percent_rank() over (partition by order_id order by shipped_date desc) as shipped_date_pct_rank
from shipments
