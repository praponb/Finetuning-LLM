select status, month(delivered_date) delivered_month, round((100 * sum(case when carrier = 'FedEx' then 1 else 0 end)) / count(*), 1) pct_fedex
from shipments
where status in ('LOST', 'DELIVERED') and carrier = 'DHL'
group by status, delivered_month
order by status desc
