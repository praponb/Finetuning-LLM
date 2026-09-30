select c.credit_limit, c.email, c.city, a.balance, a.customer_id from customers c left join accounts a on c.id = a.customer_id;
