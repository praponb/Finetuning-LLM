select started_at, case when device in ('mobile', 'desktop') then 'Priority' else 'Other' end as device_group, substring(browser, 1, 4) as browser_prefix
from sessions
