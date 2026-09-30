with base as (
    -- Keep recent, valid rows
    select *, trim(country) clean_country
    from customers
    where signup_date > date '2023-03-08'
      and segment is not null
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from base s
    where exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.opened_date >= s.signup_date
        )
),
deduped as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by city order by credit_limit) as rank_in_group
    from eligible
    qualify rank_in_group = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        name,
        email,
        country,
        segment,
        signup_date,
        is_active,
        age,
        rank_in_group,
        corr(age, credit_limit) over (partition by city) corr_age_credit_limit,
        max(credit_limit) over (partition by city, country) city_max_credit_limit,
        avg(credit_limit) over (partition by segment order by credit_limit, id rows between 2 preceding and current row) credit_limit_avg_2
    from deduped
),
with_last as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.country,
        s.signup_date,
        s.is_active,
        s.age,
        s.credit_limit_avg_2,
        latest.balance,
        latest.account_opened_date
    from with_windows s
    left join lateral (
            select c.balance, c.opened_date as account_opened_date
            from accounts c
            where c.customer_id = s.id
            order by c.opened_date desc, c.id desc
            limit 1
        ) latest on true
)
-- Multi-level aggregation
select
    case when age >= 57 then 'PREMIUM' else 'STANDARD' end as age_tier,
    country,
    size(collect_set(country)) as num_countries,
    sum(age) filter (where country = 'JP') as jp_total_age,
    avg(age) filter (where country = 'UK') as uk_avg_age,
    grouping(case when age >= 57 then 'PREMIUM' else 'STANDARD' end) as grouping_level
from with_last
group by cube (case when age >= 57 then 'PREMIUM' else 'STANDARD' end, country)
order by age_tier desc
