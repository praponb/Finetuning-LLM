SELECT email, name FROM customers WHERE country = 'UK' UNION ALL SELECT email, name FROM customers WHERE LENGTH(name) > 9 ORDER BY email
