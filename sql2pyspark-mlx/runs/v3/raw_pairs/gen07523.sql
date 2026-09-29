select id, delivered_date, ntile(10) over (order by delivered_date, id) as quartile
from shipments;
