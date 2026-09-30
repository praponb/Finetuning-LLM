SELECT c.is_active, DATE_TRUNC('MONTH', c.signup_date) AS signup_month, COUNT(*) AS total_count
FROM transactions t
JOIN accounts a ON a.id = t.account_id
FULL OUTER JOIN customers c ON a.customer_id = c.id
GROUP BY c.is_active, DATE_TRUNC('MONTH', c.signup_date)
HAVING COUNT(*) >= 2
ORDER BY total_count;
