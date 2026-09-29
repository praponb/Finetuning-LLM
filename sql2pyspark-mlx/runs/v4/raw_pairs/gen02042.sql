with Base as (
    select *
    from store_sales
    where product_id in (1, 3, 9)
),
agg as (
    select store_id, count(*) as cnt, count(revenue) as revenue_count
    from Base
    group by store_id
)
select *
from agg
where cnt < 8;
