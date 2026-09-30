select status, month(order_date) as order_month, concat_ws(', ', sort_array(collect_set(channel))) as channel_values
from orders
where customer_id in (13, 22, 31, 39) and customer_id = 13
group by status, month(order_date)
order by status, channel_values
