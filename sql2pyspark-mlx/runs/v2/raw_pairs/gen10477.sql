select o.status, count(*) as cnt from order_items oi left join orders o on oi.order_id = o.id inner join customers c on o.customer_id = c.id group by o.status;
