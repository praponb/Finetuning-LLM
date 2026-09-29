select
    i.id,
    i.product_id,
    s.opened_date,
    s.store_name
from inventory i
left join stores s on i.store_id = s.id
