with product_summary as (
    select product_id, max(quantity) as max_quantity
    from inventory
    group by product_id
)
select p.id, p.cost, ps.max_quantity
from products p
inner join product_summary ps on p.id = ps.product_id
order by p.id;
