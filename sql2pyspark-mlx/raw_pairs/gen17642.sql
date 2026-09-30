select *, last_day(delivered_date) as delivered_date_month_end, month(delivered_date) as delivered_date_month
from shipments
where carrier not in ('UPS', 'Kerry')
