select store_id, max_quantity
from (
    select store_id, max(quantity) as max_quantity
    from inventory
    where product_id in (3, 5, 7, 9)
    group by store_id
) as t
where max_quantity >= 200
order by store_id
