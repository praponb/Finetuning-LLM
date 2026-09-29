with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(rating, 1) as rating_rounded,
        round(rating, 2) as rating_rounded_2
    from suppliers
    where city is not null
)
-- Native SQL PIVOT
select *
from (
    select
        city,
        country,
        rating_rounded_2
    from cleaned
) src
pivot (
    sum(rating_rounded_2) as total,
    count(1) as n
    for country in ('UK' as uk, 'US' as us, 'TH' as th)
)
