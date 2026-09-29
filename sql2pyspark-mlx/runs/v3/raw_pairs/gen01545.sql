select c.segment, count(distinct o.amount) unique_stores from orders o inner join customers c on o.customer_id = c.id group by c.segment order by segment asc, unique_stores desc limit 5;
