select oi.id, oi.quantity, o.order_date, o.store_id, s.sqft from order_items oi join orders o on oi.order_id = o.id join stores s on o.store_id = s.id where s.opened_date >= '2024-07-01'
