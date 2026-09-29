select s.city, t.first_last_restock, t.min_last_restock
from stores s
inner join (select store_id, min(last_restock) first_last_restock, min(last_restock) min_last_restock
    from inventory
    group by store_id) t on s.id = t.store_id
