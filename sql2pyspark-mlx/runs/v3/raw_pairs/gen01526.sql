select id, discount, customer_id, case when channel = 'mobile' then 1 else 0 end as is_mobile from orders where store_id in (2, 25, 28) order by id desc limit 50
