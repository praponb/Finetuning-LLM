select
    device,
    browser,
    lower(browser) browser_lower,
    case when device in ('desktop', 'tablet') then 'Tier 1' else 'Other' end device_group
from sessions
where customer_id = 6
order by device_group;
