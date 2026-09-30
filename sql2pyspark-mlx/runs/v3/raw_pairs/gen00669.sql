select * from (select id, store_id, quantity, last_restock, rank() over (partition by store_id order by quantity desc) as rnk from inventory) as t where rnk <= 3
