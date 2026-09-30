select id, shipping_fee, status from orders where discount < 0.19 and (order_date <= '2024-05-01' or status in ('SHIPPED', 'COMPLETED', 'RETURNED')) order by id limit 20;
