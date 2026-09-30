select s.city, round(avg(i.quantity), 2) as avg_quantity from inventory i full outer join stores s on i.store_id = s.id where i.store_id in (1, 2) group by s.city order by avg_quantity desc
