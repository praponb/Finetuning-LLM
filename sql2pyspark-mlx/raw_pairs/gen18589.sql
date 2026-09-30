select id, cost, datediff(delivered_date, shipped_date) / 7 as weeks_shipped_date_to_delivered_date
from shipments
where cost >= 46;
