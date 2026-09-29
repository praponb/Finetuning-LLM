SELECT customer_id, ABS(balance) AS abs_balance
FROM accounts
WHERE status = 'OPEN' OR status NOT IN ('CLOSED', 'FROZEN', 'OPEN')
ORDER BY 1 DESC, abs_balance DESC;
