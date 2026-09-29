select id, shipping_fee, substring(channel, 1, 2) as channel_prefix
from orders;
