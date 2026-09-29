select s.city, count(distinct i.quantity) as unique_stores, min(i.quantity) as min_quantity from inventory i join stores s on i.store_id = s.id group by s.city order by s.city limit 50
