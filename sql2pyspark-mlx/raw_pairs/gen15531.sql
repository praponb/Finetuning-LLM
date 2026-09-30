select s.region, round(avg(i.quantity), 2) as avg_quantity, sum(i.quantity) as total_quantity from inventory i inner join stores s on i.store_id = s.id group by s.region order by 1 desc limit 5;
