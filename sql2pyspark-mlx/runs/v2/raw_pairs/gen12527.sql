select
    ss.id,
    ss.product_id,
    ss.sale_date,
    ss.revenue,
    s.opened_date,
    s.sqft,
    s.store_name
from store_sales ss
left join stores s on s.id = ss.store_id
