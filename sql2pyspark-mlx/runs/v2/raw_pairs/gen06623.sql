with Ranked as (
    select *, dense_rank() over (partition by country order by age desc) as rnk
    from customers
)
select
    id,
    country,
    age,
    segment
from Ranked
where rnk <= 2;
