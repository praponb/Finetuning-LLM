select s.delivered_date, o.status, c.age, c.is_active from shipments s join orders o on s.order_id = o.id join customers c on o.customer_id = c.id
