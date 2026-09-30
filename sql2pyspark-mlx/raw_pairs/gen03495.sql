SELECT c.segment, a.account_type, a.status, a.opened_date FROM customers c LEFT OUTER JOIN accounts a ON c.id = a.customer_id
