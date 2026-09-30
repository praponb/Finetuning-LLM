with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        device,
        started_at,
        trim(device) as clean_device,
        coalesce(country, 'N/A') as resolved_country
    from sessions
    where started_at > to_timestamp('2023-05-03 17:00:00')
),
matched as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from base e
    left join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.started_at between sub.start_date and sub.end_date
),
with_latest as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.web_event_page,
        latest.web_event_event_type
    from matched s
    left join lateral (
            select c.page as web_event_page, c.event_type as web_event_event_type
            from web_events c
            where c.session_id = s.id
            order by c.event_time desc, c.id desc
            limit 1
        ) latest on true
),
matched_keys as (
    -- Set difference on keys
    select customer_id, device
    from with_latest
    except
    select customer_id, device
    from sessions
    where browser <> 'Safari'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_latest s
    inner join matched_keys k on s.customer_id = k.customer_id and s.device = k.device
),
enriched as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.device,
        s.resolved_country,
        s.subscription_id,
        s.monthly_fee,
        s.web_event_page,
        s.web_event_event_type,
        p.credit_limit,
        p.age,
        p.email
    from verified s
    inner join customers p
        on s.customer_id = p.id
),
joined as (
    -- Attach customers attributes
    select
        src.*,
        p.segment,
        p.name,
        p.country
    from enriched src
    left join customers p
        on src.customer_id = p.id
    where p.signup_date > '2025-02-26'
)
-- Multi-level aggregation
select
    case when monthly_fee >= 219 then 'LARGE' else 'SMALL' end as monthly_fee_tier,
    web_event_event_type,
    case when subscription_id >= 8 then 'HIGH' else 'LOW' end as subscription_id_tier,
    round(avg(monthly_fee), 2) as avg_monthly_fee,
    approx_percentile(monthly_fee, array(0.25, 0.5, 0.75)) as monthly_fee_quartiles,
    size(collect_set(segment)) as num_segments,
    percentile_approx(subscription_id, array(0.25, 0.5, 0.75)) as subscription_id_quartiles,
    percentile_approx(age, 0.95) as age_p95
from joined
group by grouping sets ((case when monthly_fee >= 219 then 'LARGE' else 'SMALL' end, web_event_event_type, case when subscription_id >= 8 then 'HIGH' else 'LOW' end), (case when monthly_fee >= 219 then 'LARGE' else 'SMALL' end, web_event_event_type))
having round(avg(monthly_fee), 2) > 0
