select
    *,
    first_value(channel, true) over (order by shipping_fee, id rows between unbounded preceding and current row) as first_known_channel,
    cume_dist() over (partition by channel order by discount) as discount_cume_dist,
    count(discount) over w_running as status_count_discount,
    avg(shipping_fee) over (partition by status order by shipping_fee, id rows between 3 preceding and current row) as shipping_fee_avg_3,
    avg(amount) over (partition by status, store_id order by discount, id rows between 2 preceding and current row) as amount_avg_2
from orders
window w_running as (partition by status, customer_id)
