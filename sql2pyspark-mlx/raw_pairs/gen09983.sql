select id, store_id, last_restock, rank() over (partition by store_id order by last_restock) as rnk from inventory;
