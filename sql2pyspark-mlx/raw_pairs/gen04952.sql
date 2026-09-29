with metrics as (
    -- Ranking and gap features
    select
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        lead(age) over w_recent as next_age,
        lead(credit_limit) over w_recent as next_credit_limit,
        lag(credit_limit) over w_recent as prev_credit_limit,
        row_number() over w_recent as row_seq,
        last_value(segment) ignore nulls over w_hist as last_known_segment
    from customers
    window w_recent as (partition by segment order by age, id), w_hist as (partition by segment order by credit_limit, id rows between unbounded preceding and current row)
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from subscriptions x
            where x.customer_id = s.id
              and x.end_date <= s.signup_date
        )
),
with_latest as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select s.*, x.status
    from eligible s
    join lateral (
            select c.status
            from accounts c
            where c.customer_id = s.id
              and c.opened_date <= s.signup_date
            order by c.opened_date desc, c.id desc
            limit 1
        ) x on true
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        age,
        next_age,
        next_credit_limit,
        last_known_segment,
        last_value(next_age, true) over w_running as last_known_next_age,
        sum(prev_credit_limit) over (partition by city order by next_credit_limit, id rows between unbounded preceding and current row) as running_prev_credit_limit
    from with_latest
    window w_running as (partition by status, city order by row_seq, id rows between unbounded preceding and current row)
),
with_last as (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.session_country,
        top1.device
    from enriched s
    join lateral (
            select c.country as session_country, c.device
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    age,
    next_age,
    last_known_segment,
    last_known_next_age,
    running_prev_credit_limit,
    session_country,
    device,
    case when device rlike '^(mobile|desktop)$' then 'OTHER_ZONE' else 'CORE' end as device_zone
from with_last;
