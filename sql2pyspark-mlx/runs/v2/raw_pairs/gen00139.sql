select
    i.id,
    i.product_id,
    i.store_id,
    s.opened_date,
    p.brand,
    year(s.opened_date) as opened_date_year
from inventory i
right join stores s on i.store_id = s.id
inner join products p on i.product_id = p.id
