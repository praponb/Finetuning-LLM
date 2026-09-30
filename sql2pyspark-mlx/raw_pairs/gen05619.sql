select s.city, count(distinct i.quantity) as unique_stores from inventory i inner join stores s on i.store_id = s.id group by s.city order by unique_stores;
