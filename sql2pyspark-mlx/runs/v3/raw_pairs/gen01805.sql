with recent_store_sales as (
    select *
    from store_sales
    where units < 25
),
Summary as (
    select product_id, count(distinct units) as unique_stores, min(sale_date) as first_sale_date
    from recent_store_sales
    group by product_id
)
select product_id, unique_stores, first_sale_date
from Summary
where unique_stores > 7
