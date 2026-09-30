select date_format(delivered_date, 'yyyy-MM') as delivered_month, sum(case when carrier = 'DHL' then cost else 0 end) as dhl_cost
from shipments
where status in ('RETURNED', 'IN_TRANSIT', 'DELIVERED') and order_id is null
group by date_format(delivered_date, 'yyyy-MM')
