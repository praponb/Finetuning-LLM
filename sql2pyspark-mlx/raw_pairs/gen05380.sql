with valid as (
    select
        id,
        email,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        case when is_active then 'Yes' else 'No' end as is_active_label,
        credit_limit * age as credit_limit_times_age
    from customers
    where signup_date <= '2025-02-01'
),
with_last as (
    select
        s.*,
        latest.session_device,
        latest.session_country
    from valid s
    left join lateral (
            select c.device as session_device, c.country as session_country
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) latest on true
),
not_flagged as (
    select s.*
    from with_last s
    where not exists (
            select 1
            from orders x
            where x.customer_id = s.id
              and x.status = 'RETURNED'
        )
),
with_last_customers as (
    select
        s.id,
        s.email,
        s.country,
        s.is_active,
        s.is_active_label,
        s.session_country,
        x.session_country_2
    from not_flagged s
    left join lateral (
            select c.country as session_country_2
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) x on true
),
with_latest as (
    select s.*, x.channel
    from with_last_customers s
    left join lateral (
            select c.channel
            from orders c
            where c.customer_id = s.id
            order by c.discount desc, c.id desc
            limit 1
        ) x on true
)
select
    country,
    count(distinct channel) as distinct_channels,
    size(collect_set(session_country)) as num_session_countries,
    count(*) as cnt,
    count(*) as row_count,
    count(*) as event_count
from with_latest
group by country
order by cnt desc
