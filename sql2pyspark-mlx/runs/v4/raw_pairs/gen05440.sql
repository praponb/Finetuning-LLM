with cleaned as (
    select
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        price * cost as margin,
        trim(product_name) as clean_product_name,
        coalesce(brand, 'N/A') as brand_clean
    from products
    where launch_date >= date '2023-04-16'
)
select *
from (
    select *, row_number() over (partition by category order by margin, id) as latest_rank
    from cleaned
) x
where latest_rank = 1
