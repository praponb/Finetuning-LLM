select case when status in ('PAUSED', 'EXPIRED') then 'tier_1' else 'other' end as status_group, round((100 * sum(case when status = 'ACTIVE' then 1 else 0 end)) / count(*), 1) as pct_active, count(case when plan = 'enterprise' then 1 end) as enterprise_count
from subscriptions
where end_date between '2024-04-29' and '2025-05-01'
group by case when status in ('PAUSED', 'EXPIRED') then 'tier_1' else 'other' end;
