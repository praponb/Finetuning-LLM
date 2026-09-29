with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, add_months(signup_date, 12) signup_date_plus_months
    from customers
    where signup_date >= to_date('2023-06-04')
      and segment is not null
)
-- Native SQL PIVOT
select *
from (
    select
        city,
        country,
        segment,
        credit_limit
    from filtered
) src
pivot (
    max(credit_limit) as amount,
    count(1) as n
    for segment in ('SMB' as smb, 'Corporate' as corporate, 'Consumer' as consumer)
)
