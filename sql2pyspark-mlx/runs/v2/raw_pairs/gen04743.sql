select
    carrier,
    sum(cost) as total_cost,
    count(*) as cnt,
    dense_rank() over (order by sum(cost) desc) as rnk
from shipments
group by carrier
order by rnk desc;
