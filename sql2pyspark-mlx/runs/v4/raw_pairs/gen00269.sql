with top_store_sales as (
    select
        id,
        product_id,
        units,
        store_id,
        rank() over (partition by product_id order by units desc) pos
    from store_sales
)
select *
from top_store_sales
where pos <= 3
order by units desc;
