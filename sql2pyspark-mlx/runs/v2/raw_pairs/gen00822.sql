select *, date_trunc('week', started_at) started_at_period, case when browser in ('Edge', 'Safari') then 'Tier 1' else 'Other' end browser_group
from sessions
where device in ('mobile', 'desktop') and country in ('SG', 'TH', 'DE')
order by customer_id desc, id
limit 3;
