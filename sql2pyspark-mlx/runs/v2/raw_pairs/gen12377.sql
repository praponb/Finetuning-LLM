select store_id, avg_quantity, total_quantity
from (
    select store_id, round(avg(quantity), 2) as avg_quantity, sum(quantity) as total_quantity
    from inventory
    group by store_id
) as sub
where total_quantity >= 840
order by store_id desc
limit 3
