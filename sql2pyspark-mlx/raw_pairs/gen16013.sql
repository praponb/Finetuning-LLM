select s.id, s.city, s.sqft from stores s where s.id in (select store_id from orders);
