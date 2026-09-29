select orders.id, orders.status, orders.discount, customers.city, customers.country from orders join customers on orders.customer_id = customers.id where customers.email is not null;
