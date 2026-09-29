select
    id,
    order_date,
    channel,
    upper(channel) as channel_upper
from orders;
