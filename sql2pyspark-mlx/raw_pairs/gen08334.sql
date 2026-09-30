select
    status,
    order_id,
    round(stddev(amount), 2) as stddev_amount,
    min(paid_at) as first_paid_at,
    concat_ws(', ', sort_array(collect_set(status))) as status_values
from payments
where order_id = 21 or order_id is not null
group by status, order_id
order by status;
