SELECT id, segment FROM customers INTERSECT SELECT id, segment FROM customers WHERE city = 'New York'
