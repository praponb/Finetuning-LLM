with ranked_customers as (
    select *, row_number() over (partition by city order by age desc, id) as rn
    from customers
)
select
    id,
    city,
    age,
    name
from ranked_customers
where rn <= 3;
