select
    id,
    status,
    shipping_fee,
    discount,
    avg(shipping_fee) over (partition by status) as status_avg_shipping_fee,
    min(discount) over (partition by status) as status_min_discount
from orders;
