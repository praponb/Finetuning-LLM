select id, channel from orders where order_date <= '2023-10-01' union all select id, channel from orders where status in ('RETURNED', 'PENDING');
