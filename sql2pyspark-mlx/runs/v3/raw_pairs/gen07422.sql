select channel, min(order_date) as first_order_date from orders where status != 'RETURNED' and channel in ('store', 'mobile', 'partner') group by channel;
