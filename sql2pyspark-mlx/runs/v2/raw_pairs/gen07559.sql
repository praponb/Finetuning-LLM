select oi.id, oi.order_id, o.status, o.order_date from order_items oi left join orders o on o.id = oi.order_id where oi.order_id = 9 and o.amount between 430 and 740;
