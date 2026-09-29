select o.id, o.store_id, s.region, s.opened_date, c.email from orders o join stores s on o.store_id = s.id join customers c on o.customer_id = c.id order by store_id desc;
