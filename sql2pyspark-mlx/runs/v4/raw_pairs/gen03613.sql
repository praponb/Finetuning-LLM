with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from customers s
    where exists (
            select 1
            from orders ch
            where ch.customer_id = s.id
              and ch.order_date < s.signup_date
        )
),
scored as (
    -- Ranking and gap features
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        corr(credit_limit, age) over (partition by country) as corr_credit_limit_age,
        cume_dist() over (partition by city order by age nulls last) as age_cume_dist,
        avg(credit_limit) over (partition by city, segment order by credit_limit, id rows between 3 preceding and 1 preceding) as credit_limit_avg_3_prior,
        stddev_samp(credit_limit) over (partition by segment) as segment_stddev_samp_credit_limit
    from kept
),
scored_customers as (
    -- Ranking and gap features
    select
        *,
        avg(age_cume_dist) over (partition by country, segment order by age, id rows between 5 preceding and current row) as age_cume_dist_avg_5,
        round(segment_stddev_samp_credit_limit / sum(segment_stddev_samp_credit_limit) over (partition by segment), 4) as segment_stddev_samp_credit_limit_share,
        percent_rank() over (partition by segment order by segment_stddev_samp_credit_limit desc nulls last) as segment_stddev_samp_credit_limit_percent_rank
    from scored
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by city, country order by segment_stddev_samp_credit_limit_percent_rank desc, id) as latest_rank
    from scored_customers
    qualify latest_rank = 1
),
with_latest as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.status,
        x.channel
    from ranked s
    left join lateral (
            select c.status, c.channel
            from orders c
            where c.customer_id = s.id
            order by c.order_date desc, c.id desc
            limit 1
        ) x on true
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, month(signup_date) as signup_date_month
    from with_latest
    where age is null
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from accounts ch
            where ch.customer_id = s.id
              and ch.opened_date >= s.signup_date
        )
)
-- Native SQL PIVOT
select *
from (
    select
        channel,
        country,
        credit_limit_avg_3_prior
    from not_flagged
) src
pivot (
    sum(credit_limit_avg_3_prior)
    for country in ('TH', 'JP')
)
