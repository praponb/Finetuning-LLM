select c.segment, count(o.id) as order_count, min(o.order_date) as min_order_date from customers c left outer join orders o on o.customer_id = c.id group by c.segment;
