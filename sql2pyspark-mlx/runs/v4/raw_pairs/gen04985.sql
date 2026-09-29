select carrier, case when carrier in ('Kerry', 'Flash') then 'Core' else 'Other' end as carrier_group, coalesce(cost, 0) as cost_filled
from shipments
where carrier = 'Flash' and order_id is not null and shipped_date > '2023-04-01'
order by cost_filled;
