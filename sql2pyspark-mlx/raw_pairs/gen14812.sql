SELECT c.segment, c.email, a.customer_id, a.opened_date FROM customers c LEFT JOIN accounts a ON c.id = a.customer_id WHERE c.segment IN ('Corporate', 'Consumer');
