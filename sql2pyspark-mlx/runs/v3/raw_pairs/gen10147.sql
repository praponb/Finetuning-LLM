with valid_store_sales as (
    select *
    from store_sales
    where revenue <= 3200 and store_id in (1, 5)
),
summary as (
    select product_id, count(*) as cnt
    from valid_store_sales
    group by product_id
)
select *
from summary;
