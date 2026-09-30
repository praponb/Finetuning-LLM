SELECT order_date FROM orders INTERSECT SELECT signup_date FROM customers WHERE country <> 'UK'
