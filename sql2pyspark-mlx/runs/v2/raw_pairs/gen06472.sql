select date_format(delivered_date, 'yyyy-MM') as delivered_month, max(cost) - min(cost) as cost_range
from shipments
group by date_format(delivered_date, 'yyyy-MM')
having cost_range < 45;
