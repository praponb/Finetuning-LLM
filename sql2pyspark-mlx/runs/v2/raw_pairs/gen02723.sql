select carrier, min(shipped_date) as first_shipped_date
from shipments
where cost >= 87 or shipped_date > '2025-10-01'
group by carrier
order by first_shipped_date desc;
