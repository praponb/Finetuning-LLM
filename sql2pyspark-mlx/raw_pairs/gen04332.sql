select date_trunc('MONTH', delivered_date) as delivered_month, sum(case when carrier = 'FedEx' then 1 else 0 end) as fedex_count, max(cost) as max_cost
from shipments
group by date_trunc('MONTH', delivered_date)
order by max_cost desc
