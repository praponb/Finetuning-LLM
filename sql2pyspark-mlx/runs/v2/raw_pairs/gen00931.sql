with StoreSalesByProduct as (
    select product_id, count(distinct units) as unique_products
    from store_sales
    where product_id = 8
    group by product_id
)
select p.id, p.cost, S.unique_products
from products p
left outer join StoreSalesByProduct S on p.id = S.product_id
order by p.cost desc;
