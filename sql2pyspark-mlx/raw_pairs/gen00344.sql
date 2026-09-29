select id, shipped_date, substring(status, 1, 4) as status_prefix
from shipments
where cost >= 84
order by status_prefix
