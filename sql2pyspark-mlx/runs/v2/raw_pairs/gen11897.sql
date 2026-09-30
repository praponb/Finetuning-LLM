SELECT c.city, a.opened_date FROM customers c LEFT JOIN accounts a ON c.id = a.customer_id ORDER BY c.city
