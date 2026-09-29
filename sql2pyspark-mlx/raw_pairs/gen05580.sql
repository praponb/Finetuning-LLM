with top_store_sales as (
    select *, row_number() over (partition by product_id order by sale_date, id) as rnk
    from store_sales
    where revenue > 3700
)
select
    id,
    product_id,
    sale_date,
    store_id
from top_store_sales
where rnk <= 3
