with deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by country, segment order by signup_date nulls last, id) as rn
    from customers
    qualify rn = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        corr(age, credit_limit) over (partition by city) as corr_age_credit_limit,
        sum(age) over w_hist as cumulative_age,
        round(age / sum(age) over (partition by country), 4) as age_share,
        ntile(10) over w_recent as credit_limit_bucket,
        max(age) over (partition by date_trunc('YEAR', signup_date)) as signup_date_period_max_age
    from deduped
    window w_hist as (partition by segment order by signup_date, id rows between unbounded preceding and current row), w_recent as (order by credit_limit desc, id)
),
with_latest as (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.country,
        s.segment,
        s.credit_limit,
        s.age,
        s.age_share,
        top1.session_started_at,
        top1.session_device
    from enriched s
    left join lateral (
            select c.started_at as session_started_at, c.device as session_device
            from sessions c
            where c.customer_id = s.id
              and c.started_at <= s.signup_date
            order by c.started_at desc, c.id desc
            limit 1
        ) top1 on true
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when session_started_at - lag(session_started_at) over (partition by segment order by session_started_at, id) > interval '14' day then 1 else 0 end as is_new_burst
    from with_latest
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        segment,
        credit_limit,
        age,
        session_started_at,
        session_device,
        sum(is_new_burst) over (partition by segment order by session_started_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
metrics as (
    -- Ranking and gap features
    select
        *,
        first_value(segment) ignore nulls over (partition by segment order by session_started_at, id rows between unbounded preceding and current row) as first_known_segment,
        lead(session_no, 2) over (partition by year(session_started_at), session_device order by credit_limit, id) as next_session_no,
        round(credit_limit / sum(credit_limit) over (partition by session_device, segment), 4) as credit_limit_share
    from islands
),
kept_keys as (
    -- Set intersection on keys
    select segment, id
    from metrics
    intersect
    select segment, id
    from customers
    where upper(name) like '%IA %'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join kept_keys k on s.segment = k.segment and s.id = k.id
)
select
    id,
    segment,
    age,
    session_started_at,
    session_device,
    session_no,
    next_session_no,
    credit_limit_share
from reconciled
order by session_started_at desc nulls last;
