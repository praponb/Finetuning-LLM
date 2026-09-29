select id, quantity, last_restock, store_id, datediff(current_date(), last_restock) days_since_last_restock from inventory where year(last_restock) = 2024 order by store_id desc
