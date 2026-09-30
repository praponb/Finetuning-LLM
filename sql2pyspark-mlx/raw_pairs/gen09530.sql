select
    id,
    shipping_fee,
    store_id,
    trim(status) as clean_status
from orders
where shipping_fee > 13
order by id, clean_status desc
