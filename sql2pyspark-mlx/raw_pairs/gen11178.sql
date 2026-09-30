select carrier, count(*) as num_shipments
from shipments
group by carrier
order by carrier
