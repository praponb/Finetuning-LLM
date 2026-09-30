select
    id,
    store_id,
    order_date,
    status,
    sum(discount) over w_hist as last7_sum_discount,
    count(discount) over w_hist as last7_count_discount,
    min(discount) over w_hist as last7_min_discount,
    max(discount) over w_hist as last7_max_discount,
    row_number() over (partition by channel order by order_date desc, id) as seq_no
from orders
where customer_id = 16
window w_hist as (partition by status order by discount, id rows between 6 preceding and current row);
