select *
from (
    select product_id, count(quantity) as quantity_count
    from inventory
    group by product_id
) as s
where quantity_count < 4
order by quantity_count desc, product_id desc;
