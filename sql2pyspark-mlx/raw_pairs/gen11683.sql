select
    i.id,
    i.last_restock,
    s.store_name,
    p.brand
from inventory i
inner join stores s on i.store_id = s.id
inner join products p on p.id = i.product_id
order by i.id desc
limit 50
