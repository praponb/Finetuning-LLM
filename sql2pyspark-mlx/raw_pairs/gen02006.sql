with cleaned as (
    select
        *,
        length(product_name) as product_name_length,
        coalesce(brand, 'N/A') as resolved_brand
    from products
    where launch_date > date '2023-05-23'
      and brand is distinct from 'Initech'
)
select
    id,
    brand,
    tags,
    product_name_length,
    resolved_brand
from (
    select *, row_number() over (partition by category order by launch_date, id) as rn
    from cleaned
) t
where rn = 1
