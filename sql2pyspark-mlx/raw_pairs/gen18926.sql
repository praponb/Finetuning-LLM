select
    p.launch_date,
    i.store_id,
    i.quantity,
    i.product_id
from products p
left join inventory i on p.id = i.product_id
order by i.product_id desc;
