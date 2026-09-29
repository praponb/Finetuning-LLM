with base as (
    select *
    from products
    where array_contains(tags, 'eco')
),
agg as (
    select brand, count(distinct price) as unique_categorys
    from base
    group by brand
)
select *
from agg
order by 1;
