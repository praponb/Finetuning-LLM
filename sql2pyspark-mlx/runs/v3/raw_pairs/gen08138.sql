select p.id, p.product_name, t.max_quantity
from products p
inner join (select product_id, max(quantity) as max_quantity
    from inventory
    where product_id in (2, 5)
    group by product_id) t on p.id = t.product_id
order by max_quantity desc, id
limit 5
