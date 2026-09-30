select
    p.product_name,
    p.brand,
    i.quantity,
    i.product_id,
    i.store_id
from products p
left join inventory i on i.product_id = p.id
where i.quantity <= 189
order by product_id;
