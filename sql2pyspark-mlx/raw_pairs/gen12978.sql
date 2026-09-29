select *, substring(channel, 1, 2) as channel_prefix from orders where discount <= 0.31;
