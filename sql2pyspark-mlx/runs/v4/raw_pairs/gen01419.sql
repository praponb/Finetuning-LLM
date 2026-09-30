select i.id, i.last_restock, s.store_name, s.city from inventory i right join stores s on i.store_id = s.id where s.sqft <= 12145 and i.store_id in (1, 2, 4, 5) order by city
