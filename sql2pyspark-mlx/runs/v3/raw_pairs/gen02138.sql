select store_id, min(last_restock) as first_last_restock from inventory where month(last_restock) in (4, 8, 11) group by store_id order by first_last_restock desc;
