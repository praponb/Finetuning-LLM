select order_id, carrier, max(cost) - min(cost) as cost_range
from shipments
where status in ('DELIVERED', 'LOST') and (year(shipped_date) >= 2025 or shipped_date between '2024-02-01' and '2024-09-01')
group by order_id, carrier
having sum(case when status = 'RETURNED' then 1 else 0 end) > 28
