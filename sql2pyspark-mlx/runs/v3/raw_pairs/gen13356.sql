select c.id, c.name, count(o.id) as order_count from customers c join orders o on c.id = o.customer_id group by c.id, c.name order by order_count desc;
