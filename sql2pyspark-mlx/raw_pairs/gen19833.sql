select s.id, s.sqft, t.latest_order_date from stores s inner join (select store_id, max(order_date) as latest_order_date from orders group by store_id) t on s.id = t.store_id
