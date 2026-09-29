select i.id, i.product_id, i.last_restock, i.quantity, s.region from inventory i left join stores s on i.store_id = s.id order by last_restock, id limit 10
