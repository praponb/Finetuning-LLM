SELECT a.id, a.customer_id, a.status
FROM accounts a
WHERE MONTH(a.opened_date) IN (7, 8, 9) AND a.customer_id IN (SELECT id FROM customers WHERE city NOT IN ('New York', 'Bangkok'))
