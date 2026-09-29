with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        round(age, 1) as age_rounded,
        concat_ws(',', country, name) as country_name_key
    from customers
    where signup_date > date '2023-05-07'
      and email is not null
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (order by age nulls first) as age_dense_rank,
        ntile(4) over (partition by country order by age desc, id) as age_bucket
    from cleaned
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by segment, city order by age_rounded nulls last, id) as rn
        from with_windows
    ) x
    where rn = 1
)
-- Native SQL PIVOT
select *
from (
    select
        country,
        segment,
        age
    from latest
) src
pivot (
    sum(age)
    for segment in ('Corporate', 'SMB')
)
