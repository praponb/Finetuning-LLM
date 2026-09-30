select store_id, product_id, min(quantity) as min_quantity
from inventory
where product_id in (6, 7, 8)
group by store_id, product_id
having min(quantity) < 130
order by product_id desc
