SELECT YEAR(c.signup_date) AS signup_year, MAX(p.paid_at) AS last_paid_at, COUNT(p.amount) AS amount_count
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
JOIN customers c ON c.id = o.customer_id
WHERE c.email LIKE '%@mail.co.th'
GROUP BY YEAR(c.signup_date)
HAVING COUNT(p.amount) > 2
ORDER BY signup_year
LIMIT 20
