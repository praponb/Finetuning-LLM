with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        country,
        city,
        signup_date,
        age,
        credit_limit - age credit_limit_minus_age
    from customers
    where signup_date > date '2023-03-10'
      and email like '%@startup.io'
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        country,
        ntile(10) over (partition by city, country order by credit_limit_minus_age, id) credit_limit_minus_age_bucket,
        rank() over (partition by city, country order by credit_limit_minus_age desc) credit_limit_minus_age_rank,
        ntile(3) over w_grp age_bucket
    from cleaned
    window w_grp as (partition by city order by age, id)
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        ntile(3) over (partition by country order by credit_limit_minus_age_bucket desc, id) credit_limit_minus_age_bucket_bucket,
        rank() over (partition by country order by age_bucket desc) age_bucket_rank,
        ntile(3) over (partition by country order by age_bucket desc, id) age_bucket_bucket
    from enriched
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.balance between 59000 and 66000
        )
),
cleaned_customers as (
    -- Keep recent, valid rows
    select
        id,
        credit_limit_minus_age_bucket,
        credit_limit_minus_age_rank,
        age_bucket,
        credit_limit_minus_age_bucket_bucket,
        age_bucket_rank,
        age_bucket_bucket,
        round(credit_limit_minus_age_bucket * 1.1, 2) adjusted_credit_limit_minus_age_bucket
    from eligible
    where age_bucket <= 7
),
with_last as (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.subscription_end_date,
        top1.plan
    from cleaned_customers s
    left join lateral (
            select c.end_date as subscription_end_date, c.plan
            from subscriptions c
            where c.customer_id = s.id
            order by c.end_date desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    credit_limit_minus_age_rank,
    age_bucket,
    age_bucket_rank,
    adjusted_credit_limit_minus_age_bucket
from with_last
order by age_bucket desc nulls first, age_bucket_rank;
