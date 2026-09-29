select *
from (
    select product_id, sum(quantity) as total_quantity, count(quantity) as quantity_count
    from inventory
    where store_id in (2, 3, 4, 6)
    group by product_id
) as x
where quantity_count < 9
order by total_quantity;
