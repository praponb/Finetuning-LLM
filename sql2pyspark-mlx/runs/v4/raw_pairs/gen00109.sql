with base as (
    -- Keep recent, valid rows
    select *, date_trunc('week', started_at) as started_at_period
    from sessions
    where started_at >= timestamp '2023-08-07 06:00:00'
),
remaining_keys as (
    -- Set difference on keys
    select browser, id
    from base
    except
    select browser, id
    from sessions
    where country not in ('UK', 'SG')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join remaining_keys k on s.browser = k.browser and s.id = k.id
),
matched_keys as (
    -- Set difference on keys
    select id
    from kept_rows
    except distinct
    select id
    from sessions
    where customer_id is not null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept_rows s
    join matched_keys k on s.id = k.id
),
enriched as (
    -- Attach customers attributes
    select
        a.*,
        c.email,
        c.name
    from reconciled a
    left join customers c
        on a.customer_id = c.id
),
kept_keys as (
    -- Set difference on keys
    select country
    from enriched
    except all
    select country
    from sessions
    where device != 'mobile'
),
kept_rows_sessions as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    join kept_keys k on s.country = k.country
),
enriched_sessions as (
    -- Attach customers attributes
    select a.*, c.name as customer_name
    from kept_rows_sessions a
    join customers c
        on a.customer_id = c.id
)
-- Aggregate with conditional metrics
select
    browser,
    max(started_at_period) as ended_at,
    count(case when device = 'mobile' then 1 end) as mobile_count,
    count_if(country = 'DE') as de_events,
    (unix_timestamp(max(started_at_period)) - unix_timestamp(min(started_at_period))) / 60 as span_minutes,
    count(distinct id) as distinct_ids,
    count_if(country = 'US') as us_events
from enriched_sessions
group by browser
order by distinct_ids asc;
