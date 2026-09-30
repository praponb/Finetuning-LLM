with ranked as (
    select *
    from customers
    qualify row_number() over (partition by city order by signup_date desc nulls last, id) = 1
)
select
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit
from ranked
order by country nulls last;
