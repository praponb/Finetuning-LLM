with Src as (
    select *
    from store_sales
    where month(sale_date) in (3, 9, 10)
),
agg as (
    select product_id, store_id, count(distinct revenue) unique_stores
    from Src
    group by product_id, store_id
)
select product_id, store_id, unique_stores
from agg
where unique_stores >= 8
order by store_id desc
