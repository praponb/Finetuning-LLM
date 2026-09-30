select
    id,
    country,
    device,
    browser,
    date_trunc('year', started_at) as started_at_period,
    substring(country, 1, 3) as country_prefix
from sessions
where device = 'desktop' and started_at >= '2023-06-01'
