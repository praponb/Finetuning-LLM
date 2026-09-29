select c.id, c.is_active, t.latest_order_date from customers c left join (select customer_id, max(order_date) as latest_order_date from orders group by customer_id) t on c.id = t.customer_id;
