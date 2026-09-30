with ranked_products as (
    select
        id,
        category,
        cost,
        supplier_id,
        row_number() over (partition by category order by cost desc, id) as rn
    from products
)
select
    id,
    category,
    cost,
    supplier_id
from ranked_products
where rn <= 2
order by category;
