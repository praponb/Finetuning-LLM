select
    id,
    amount,
    store_id,
    date_format(order_date, 'yyyy-MM') as order_date_label
from orders
