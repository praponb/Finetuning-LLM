select 'shipment' as kind, delivered_date
from shipments
where status in ('DELIVERED', 'IN_TRANSIT', 'LOST')
union all
select 'subscription' as kind, end_date as delivered_date
from subscriptions
where start_date between '2023-10-01' and '2024-01-01'
order by kind desc
