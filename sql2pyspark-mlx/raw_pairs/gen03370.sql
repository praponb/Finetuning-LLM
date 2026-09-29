with not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from customers s
    where exists (
            select 1
            from accounts ch
            where ch.customer_id = s.id
              and ch.opened_date >= s.signup_date
        )
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from orders x
            where x.customer_id = s.id
              and x.order_date >= s.signup_date
        )
),
scored as (
    -- Ranking and gap features
    select
        *,
        sum(age) over w_hist as cumulative_age,
        lead(credit_limit, 2) over (partition by month(signup_date), city order by age, id) as next_credit_limit,
        avg(age) over (partition by year(signup_date) order by signup_date, id rows between 6 preceding and 1 preceding) as age_avg_6_prior
    from eligible
    window w_hist as (partition by segment order by signup_date, id rows between unbounded preceding and current row)
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        name,
        email,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        age,
        age_avg_6_prior,
        upper(segment) as segment_upper
    from scored
    where signup_date >= date '2023-05-19'
      and age_avg_6_prior between 46 and 49
)
select
    id,
    name,
    email,
    country,
    signup_date,
    credit_limit,
    age,
    age_avg_6_prior,
    segment_upper
from base
