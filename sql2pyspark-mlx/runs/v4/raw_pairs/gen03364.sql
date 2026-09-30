SELECT status, ROUND(AVG(balance), 2) avg_balance, MIN(opened_date) first_opened_date
FROM accounts
WHERE opened_date >= '2024-08-01' OR customer_id IS NOT NULL
GROUP BY status
ORDER BY first_opened_date ASC;
