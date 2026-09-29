with Base as (
    select *
    from inventory
    where product_id in (1, 2, 6, 9) and store_id in (3, 5)
),
summary as (
    select store_id, product_id, max(quantity) as max_quantity
    from Base
    group by store_id, product_id
)
select store_id, product_id, max_quantity
from summary
where max_quantity > 160
