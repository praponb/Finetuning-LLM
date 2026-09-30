select *
from (
    select
        country,
        segment,
        credit_limit
    from customers
) src
pivot (
    avg(credit_limit) as value_sum
    for segment in ('Consumer', 'Corporate')
)
