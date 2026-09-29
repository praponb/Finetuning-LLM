select
    carrier,
    sum(cost) as total_cost,
    count(*) as cnt,
    rank() over (order by sum(cost) desc) as rnk
from shipments
group by carrier
order by carrier asc, rnk desc;
