select s.sqft, s.store_name, s.region, o.customer_id, o.discount from stores s left outer join orders o on o.store_id = s.id;
