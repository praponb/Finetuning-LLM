with cleaned as (
    select
        *,
        concat_ws('-', device, country) as device_country_key,
        to_date(started_at) as started_at_date
    from sessions
    where customer_id is not null
      and device in ('tablet', 'mobile', 'desktop')
)
select
    date_trunc('year', started_at_date) as year_start,
    customer_id,
    min(started_at) as first_seen,
    count(distinct country) as distinct_countries,
    max(started_at) as last_seen
from cleaned
group by date_trunc('year', started_at_date), customer_id;
