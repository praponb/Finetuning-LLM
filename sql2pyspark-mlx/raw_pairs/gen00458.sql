with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        lower(browser) browser_lower,
        upper(device) device_upper,
        coalesce(country, 'NONE') resolved_country
    from sessions
    where customer_id is not null
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from base
    qualify row_number() over (partition by country, device order by started_at, id) = 1
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        started_at,
        country,
        device_upper,
        resolved_country,
        initcap(trim(browser)) browser_title
    from ranked
    where customer_id is not null
      and started_at >= timestamp '2023-02-18 17:00:00'
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        country,
        device_upper,
        browser_title,
        last_value(country, true) over w_part last_known_country,
        last_value(country) ignore nulls over w_part last_known_country_2
    from filtered
    where customer_id is not null
    window w_part as (partition by country order by started_at, id rows between unbounded preceding and current row)
)
-- Grouped metrics
select
    customer_id,
    count(distinct country) as distinct_countries,
    size(collect_set(country)) as num_countries,
    count(*) as num_rows,
    count(case when country = 'UK' then 1 end) as uk_count,
    count(*) as event_count,
    count_if(country = 'SG') as sg_events
from enriched
group by customer_id
