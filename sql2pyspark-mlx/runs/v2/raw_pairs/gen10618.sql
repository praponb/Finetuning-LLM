with ranked_customers as (
    select *, row_number() over (partition by country order by credit_limit, id) as pos
    from customers
)
select id, country, credit_limit
from ranked_customers
where pos = 1
order by country, credit_limit
