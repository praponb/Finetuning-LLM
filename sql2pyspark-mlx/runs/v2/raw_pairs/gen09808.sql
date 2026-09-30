select
    id,
    started_at,
    browser,
    customer_id,
    date_format(started_at, 'yyyy-MM-dd') started_at_label,
    lower(country) country_lower
from sessions
where to_date(started_at) >= '2023-07-29'
order by id desc, started_at
limit 10
