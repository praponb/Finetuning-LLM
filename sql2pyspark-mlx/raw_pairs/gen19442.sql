select carrier, sum(cost) as total_cost, dense_rank() over (order by sum(cost) desc) as rnk
from shipments
group by carrier
