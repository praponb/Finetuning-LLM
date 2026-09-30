with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        lower(browser) as browser_lower,
        trim(country) as clean_country
    from sessions
    where country is distinct from 'SG'
      and country is not null
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(country) as clean_country_2,
        concat_ws('-', browser, country) as browser_country_key,
        coalesce(browser, 'OTHER') as browser_filled
    from cleaned
    where started_at >= to_date('2023-06-16')
),
matched_keys as (
    -- Set intersection on keys
    select customer_id
    from filtered
    intersect
    select customer_id
    from sessions
    where hour(started_at) between 11 and 18
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join matched_keys k on s.customer_id = k.customer_id
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from verified s
    where exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time >= s.started_at
        )
)
select
    s.id,
    s.customer_id,
    s.browser,
    s.country,
    s.browser_lower,
    s.clean_country,
    s.browser_filled,
    (select avg(x.monthly_fee) from subscriptions x where x.customer_id = s.customer_id) as avg_subscription_monthly_fee
from eligible s
