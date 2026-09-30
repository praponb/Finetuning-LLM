select p.product_name, p.price, t.num_inventory
from products p
inner join (select product_id, count(*) as num_inventory
    from inventory
    group by product_id) t on p.id = t.product_id
