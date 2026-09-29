select id, channel, order_date from orders where store_id is null union all select id, channel, order_date from orders where status <> 'CANCELLED' order by order_date desc;
