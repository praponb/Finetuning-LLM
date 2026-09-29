with ranked as (
    select *, row_number() over (partition by brand order by cost desc, id) as pos
    from products
)
select
    id,
    brand,
    cost,
    price
from ranked
where pos <= 3
