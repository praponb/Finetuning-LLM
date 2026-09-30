select
    quarter(delivered_date) as delivered_quarter,
    sum(cost) as total_cost,
    count(*) as cnt,
    dense_rank() over (order by sum(cost) desc) as rnk
from shipments
where shipped_date > '2024-07-01'
group by quarter(delivered_date)
order by cnt desc
