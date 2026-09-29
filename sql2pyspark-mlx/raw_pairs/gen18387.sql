with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        name,
        email,
        city,
        segment,
        is_active,
        credit_limit,
        replace(name, ' ', '_') as name_slug,
        greatest(age, credit_limit) as greatest_age_credit_limit,
        coalesce(segment, 'NONE') as segment_filled
    from customers
    where signup_date >= date '2023-05-14'
      and city is distinct from 'Chiang Mai'
)
-- Native SQL PIVOT
select *
from (
    select
        segment,
        city,
        greatest_age_credit_limit
    from valid
) src
pivot (
    max(greatest_age_credit_limit) as total,
    count(1) as n
    for city in ('London', 'Tokyo')
);
