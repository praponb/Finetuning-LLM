with product_summary as (
    select product_id, min(last_restock) as min_last_restock
    from inventory
    group by product_id
)
select p.brand, p.launch_date, ps.min_last_restock
from products p
left outer join product_summary ps on p.id = ps.product_id
order by p.brand
