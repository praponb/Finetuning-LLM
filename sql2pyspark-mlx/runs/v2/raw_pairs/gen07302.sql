SELECT t.is_flagged, t.merchant, a.account_type, c.email, c.country FROM transactions t INNER JOIN accounts a ON t.account_id = a.id RIGHT JOIN customers c ON a.customer_id = c.id ORDER BY 4
