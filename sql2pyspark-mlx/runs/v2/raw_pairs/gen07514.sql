select c.id, c.name, count(o.id) as order_count, count(o.amount) as amount_count from customers c left join orders o on c.id = o.customer_id where c.name like 'Ivy%' group by c.id, c.name;
