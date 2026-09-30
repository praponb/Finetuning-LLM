select o.id, o.customer_id, o.store_id, s.opened_date, s.sqft from orders o join stores s on o.store_id = s.id
