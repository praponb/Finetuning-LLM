select
    id,
    product_name,
    category,
    price,
    cost,
    tags,
    supplier_id
from (
    select *, row_number() over (partition by supplier_id order by price nulls last, id) as rn
    from products
) t
where rn = 1
