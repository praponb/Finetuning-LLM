select i.id, i.quantity, i.store_id, s.store_name, s.sqft from inventory i join stores s on s.id = i.store_id order by quantity
