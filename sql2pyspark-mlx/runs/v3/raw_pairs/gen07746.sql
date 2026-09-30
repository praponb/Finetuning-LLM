with ranked as (
    select
        id,
        country,
        credit_limit,
        rank() over (partition by country order by credit_limit) as rn
    from customers
    where signup_date between '2023-05-01' and '2023-06-01'
)
select id, country, credit_limit
from ranked
where rn <= 2
