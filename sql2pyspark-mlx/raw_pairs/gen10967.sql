select o.id, o.store_id, o.order_date, c.country from orders o left join customers c on c.id = o.customer_id where c.city != 'Singapore' order by order_date;
