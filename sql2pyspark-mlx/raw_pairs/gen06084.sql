SELECT account_type, MIN(opened_date) AS first_opened_date, SUM(CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count
FROM accounts
WHERE customer_id IS NULL
GROUP BY account_type
ORDER BY account_type DESC
LIMIT 50;
