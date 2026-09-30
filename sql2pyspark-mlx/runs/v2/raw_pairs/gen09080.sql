with ranked_store_sales as (
    select
        id,
        product_id,
        revenue,
        row_number() over (partition by product_id order by revenue, id) as row_num
    from store_sales
)
select *
from ranked_store_sales
where row_num <= 2
