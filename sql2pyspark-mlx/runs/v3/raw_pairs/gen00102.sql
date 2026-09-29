select customer_id, started_at, case when device in ('tablet', 'mobile') then 'Priority' else 'Other' end as device_group
from sessions
where not (customer_id in (14, 15, 26, 33) or device not in ('tablet', 'desktop'))
order by device_group
