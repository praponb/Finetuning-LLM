with cleaned as (
    -- Basic cleansing and defaults
    select *, coalesce(country, 'Unknown') as country_filled
    from sessions
    where browser = 'Firefox'
),
joined as (
    -- Attach customers attributes
    select
        e.*,
        d.age,
        d.country as customer_country,
        d.city
    from cleaned e
    join customers d
        on e.customer_id <=> d.id
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from joined s
    where exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time < s.started_at
        )
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over w_recent as event_seq,
        first_value(city) ignore nulls over (order by started_at, id rows between unbounded preceding and current row) as first_known_city,
        first_value(country, true) over (partition by country, device order by started_at, id rows between unbounded preceding and current row) as first_known_country,
        avg(age) over w_grp as age_avg_7d
    from kept
    window w_recent as (partition by customer_id order by age, id), w_grp as (partition by city order by cast(started_at as bigint) range between 604800 preceding and current row)
),
with_last as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.device,
        s.browser,
        s.started_at,
        s.age,
        s.event_seq,
        s.age_avg_7d,
        x.event_type,
        x.page
    from metrics s
    left join lateral (
            select c.event_type, c.page
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
            order by c.event_time desc, c.id desc
            limit 1
        ) x on true
),
with_window as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from with_last a
    left join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.started_at >= sub.start_date
       and a.started_at < sub.end_date + interval 7 days
),
daily as (
    -- Grouped metrics
    select
        event_type,
        date_trunc('year', started_at) as year_start,
        sum(subscription_id) filter (where browser = 'Firefox') as firefox_total_subscription_id,
        count(case when device = 'desktop' then 1 end) as desktop_count,
        max(subscription_id) filter (where plan = 'standard') as standard_max_subscription_id,
        round(sum(age) / nullif(count(case when plan = 'premium' then 1 end), 0), 2) as age_per_premium,
        count_if(event_type = 'page_view') as page_view_events
    from with_window
    group by event_type, date_trunc('year', started_at)
)
select
    event_type,
    year_start,
    firefox_total_subscription_id,
    desktop_count,
    age_per_premium,
    case when event_type rlike '^(click|page_view)$' then 'GROUP_A' when event_type rlike '^(add_to_cart|purchase)$' then 'PRIORITY' else 'EXTENDED' end as event_type_zone
from daily
order by event_type_zone desc nulls last, year_start;
