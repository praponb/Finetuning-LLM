SELECT *, UPPER(status) AS status_upper, ROUND(balance, 0) AS balance_rounded
FROM accounts
ORDER BY customer_id ASC, id ASC
LIMIT 50
