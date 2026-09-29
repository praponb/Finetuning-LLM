select
    i.product_id,
    s.region,
    s.opened_date,
    p.price
from inventory i
left join stores s on i.store_id = s.id
left join products p on p.id = i.product_id
