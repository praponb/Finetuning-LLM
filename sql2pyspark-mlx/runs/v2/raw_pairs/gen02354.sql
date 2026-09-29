select id, customer_id, discount, amount, concat(channel, '/', status) as channel_status from orders where status = 'SHIPPED';
