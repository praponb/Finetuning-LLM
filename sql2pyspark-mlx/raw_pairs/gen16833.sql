with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        name,
        city,
        segment,
        signup_date,
        is_active,
        age,
        round(age / credit_limit, 2) as age_per_credit_limit,
        coalesce(city, 'UNKNOWN') as city_clean
    from customers
    where country <> 'TH'
)
-- Keep one row per key (latest first)
select *
from (
    select *, row_number() over (partition by segment order by age nulls first, id) as dedup_rank
    from valid
) ranked
where dedup_rank = 1
