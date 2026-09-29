with ranked_products as (
    select
        id,
        category,
        launch_date,
        product_name,
        dense_rank() over (partition by category order by launch_date desc) as row_num
    from products
)
select
    id,
    category,
    launch_date,
    product_name
from ranked_products
where row_num = 1
order by 1 desc;
