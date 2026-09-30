select channel, case when channel = 'web' then 1 else 0 end as is_web from orders where discount <= 0.37 and status = 'CANCELLED';
