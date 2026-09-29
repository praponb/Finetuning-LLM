with ranked_customers as (
    select
        id,
        country,
        credit_limit,
        row_number() over (partition by country order by credit_limit, id) as row_num
    from customers
    where age = 49
)
select id, country, credit_limit
from ranked_customers
where row_num <= 2
order by 3 desc;
