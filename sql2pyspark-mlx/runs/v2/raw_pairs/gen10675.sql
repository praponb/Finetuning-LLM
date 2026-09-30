select o.id, o.amount, o.store_id, s.sqft, s.store_name from orders o inner join stores s on s.id = o.store_id where o.amount > 440 order by s.sqft, id limit 10;
