with matched_keys as (
    -- Set intersection on keys
    select id
    from sessions
    intersect all
    select id
    from sessions
    where country = 'US'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessions s
    inner join matched_keys k on s.id = k.id
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from reconciled s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time >= s.started_at
        )
),
enriched as (
    -- Attach customers attributes
    select
        s.*,
        d.age,
        d.segment,
        d.city
    from kept s
    left join customers d
        on s.customer_id = d.id
),
with_dims as (
    -- Attach customers attributes
    select
        a.*,
        p.country as customer_country,
        p.name,
        p.age as customer_age
    from enriched a
    left join customers p
        on a.customer_id = p.id
),
summary as (
    -- Aggregate with conditional metrics
    select
        case when age >= 65 then 'HIGH' else 'LOW' end as age_tier,
        browser,
        date_trunc('week', started_at) as week_start,
        approx_percentile(age, 0.9) as age_p90,
        count_if(country = 'US') as us_events
    from with_dims
    group by cube (case when age >= 65 then 'HIGH' else 'LOW' end, browser, date_trunc('week', started_at))
)
select
    age_tier,
    week_start,
    us_events
from summary
order by week_start, age_tier desc;
