select segment, country, credit_limit_count
from (
    select segment, country, count(credit_limit) as credit_limit_count
    from customers
    where age is not null
    group by segment, country
) as agg
where credit_limit_count < 7
order by country;
