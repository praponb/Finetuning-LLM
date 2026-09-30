select
    status,
    month(order_date) as order_month,
    concat_ws(', ', sort_array(collect_set(status))) as status_values,
    max(order_date) as last_order_date
from orders
group by status, month(order_date)
order by status_values desc;
