with ranked_customers as (
    select
        id,
        city,
        signup_date,
        name,
        row_number() over (partition by city order by signup_date, id) as rnk
    from customers
)
select
    id,
    city,
    signup_date,
    name
from ranked_customers
where rnk <= 1;
