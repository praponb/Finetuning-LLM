select
    channel,
    amount,
    discount,
    date_format(order_date, 'yyyy-MM-dd') as order_date_label
from orders;
