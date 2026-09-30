select *, case when status in ('ACTIVE', 'EXPIRED') then 'Group A' else 'Other' end as status_group
from subscriptions
where end_date between '2024-08-04' and '2025-08-16'
order by plan;
