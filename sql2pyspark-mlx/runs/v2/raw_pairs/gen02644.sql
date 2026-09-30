select id, shipped_date
from shipments
where shipped_date >= (select max(shipped_date) from shipments)
