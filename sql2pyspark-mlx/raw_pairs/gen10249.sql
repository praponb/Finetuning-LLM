select id, status, discount, case when channel = 'web' then 1 else 0 end is_web from orders where channel not in ('web', 'store', 'mobile') order by 1 limit 10;
