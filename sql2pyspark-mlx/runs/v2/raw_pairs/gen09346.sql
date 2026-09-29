select month(delivered_date) as delivered_month, sum(case when carrier = 'Kerry' then 1 else 0 end) as kerry_count, sum(case when status = 'RETURNED' then 1 else 0 end) as returned_count
from shipments
group by delivered_month
