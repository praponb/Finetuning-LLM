with ranked as (
    select *, dense_rank() over (partition by region order by sqft desc) as pos
    from stores
)
select *
from ranked
where pos <= 5
