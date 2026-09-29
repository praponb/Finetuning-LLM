SELECT MONTH(opened_date) opened_month, account_type, COUNT(CASE WHEN status = 'OPEN' THEN 1 END) open_count
FROM accounts
WHERE account_type != 'SAVINGS'
GROUP BY MONTH(opened_date), account_type
ORDER BY account_type DESC, opened_month ASC
LIMIT 5
