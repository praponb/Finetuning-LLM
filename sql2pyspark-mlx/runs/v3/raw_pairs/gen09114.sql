with ranked_products as (
    select
        id,
        supplier_id,
        cost,
        brand,
        row_number() over (partition by supplier_id order by cost desc, id) as rnk
    from products
    where price <= 1140
)
select *
from ranked_products
where rnk <= 1
order by id
