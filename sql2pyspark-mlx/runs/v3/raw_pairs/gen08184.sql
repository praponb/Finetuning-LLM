select oi.order_id, o.customer_id, o.order_date, s.opened_date, s.sqft from order_items oi left join orders o on oi.order_id = o.id join stores s on o.store_id = s.id;
