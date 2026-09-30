select id, store_id, amount, discount from (select *, rank() over (partition by store_id order by amount desc) as rn from orders where status = 'SHIPPED') as t where rn = 1 order by amount desc, 1
