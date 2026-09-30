select store_id, min(last_restock) as first_last_restock, count(*) as total_count from inventory group by store_id order by first_last_restock desc
