SELECT customers.city, customers.name, sessions.customer_id, sessions.country FROM customers JOIN sessions ON customers.id = sessions.customer_id ORDER BY sessions.country ASC
