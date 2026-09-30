with ranked as (
    select
        id,
        product_id,
        sale_date,
        revenue,
        row_number() over (partition by product_id order by sale_date desc, id) pos
    from store_sales
)
select
    id,
    product_id,
    sale_date,
    revenue
from ranked
where pos <= 3
