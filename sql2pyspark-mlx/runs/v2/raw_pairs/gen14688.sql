select
    carrier,
    status,
    max(cost) - min(cost) as cost_range,
    count(cost) as cost_count
from shipments
where month(delivered_date) in (5, 6, 8)
group by carrier, status
having count(cost) > 1
