select
    id,
    country,
    device,
    initcap(trim(country)) country_title,
    case when device in ('mobile', 'desktop') then 'Core' else 'Other' end device_group
from sessions
where started_at >= '2023-11-15' or browser = 'Safari'
