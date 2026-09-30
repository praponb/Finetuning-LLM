with not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from customers s
    where not exists (
            select 1
            from subscriptions ch
            where ch.customer_id = s.id
              and ch.end_date <= s.signup_date
        )
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        city,
        signup_date,
        is_active,
        credit_limit,
        count(age) over (partition by country order by signup_date asc, id asc rows between 2 preceding and current row) as last3_count_age,
        max(age) over (partition by country order by signup_date asc, id asc rows between 2 preceding and current row) as last3_max_age,
        avg(age) over (partition by country order by signup_date asc, id asc rows between 2 preceding and current row) as last3_avg_age
    from not_flagged
    where credit_limit > 15600
),
with_latest as (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.session_device,
        latest.session_country
    from enriched s
    left join lateral (
            select c.device as session_device, c.country as session_country
            from sessions c
            where c.customer_id = s.id
              and c.started_at <= s.signup_date
            order by c.started_at desc, c.id desc
            limit 1
        ) latest on true
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        min(last3_max_age) over w_recent as running_min_last3_max_age,
        sum(last3_max_age) over w_recent as running_sum_last3_max_age,
        max(last3_max_age) over w_recent as running_max_last3_max_age,
        avg(last3_count_age) over w_ordered as session_country_avg_last3_count_age
    from with_latest
    window w_recent as (partition by session_device order by last3_count_age asc, id asc rows between unbounded preceding and current row), w_ordered as (partition by session_country)
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by session_country order by running_max_last3_max_age desc, id asc) as dedup_rank
    from metrics
    qualify dedup_rank = 1
)
select
    id,
    city,
    signup_date,
    last3_count_age,
    last3_max_age,
    session_device,
    running_min_last3_max_age,
    session_country_avg_last3_count_age,
    dedup_rank
from ranked
