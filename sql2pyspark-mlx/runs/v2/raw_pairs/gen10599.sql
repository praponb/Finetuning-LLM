with StoreSalesByProduct as (
    select product_id, count(distinct units) as unique_stores
    from store_sales
    group by product_id
)
select
    p.id,
    p.category,
    p.cost,
    S.unique_stores
from products p
left join StoreSalesByProduct S on p.id = S.product_id
