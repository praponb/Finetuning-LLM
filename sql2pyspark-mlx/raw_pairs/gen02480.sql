select
    i.id,
    i.product_id,
    i.store_id,
    i.quantity,
    p.price,
    p.product_name
from inventory i
left join products p on p.id = i.product_id
order by i.id desc
limit 1;
