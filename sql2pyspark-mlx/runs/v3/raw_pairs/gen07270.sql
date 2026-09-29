select carrier, min(cost) as min_cost
from shipments
where carrier is not null
group by carrier
