with product_summary as (
    select product_id, max(quantity) max_quantity
    from inventory
    where last_restock between '2023-03-01' and '2024-04-01'
    group by product_id
)
select p.id, p.supplier_id, ps.max_quantity
from products p
left join product_summary ps on p.id = ps.product_id
where ps.max_quantity >= 70
order by max_quantity desc
