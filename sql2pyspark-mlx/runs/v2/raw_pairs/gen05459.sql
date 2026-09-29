SELECT QUARTER(opened_date) opened_quarter, MAX(balance) max_balance
FROM accounts
WHERE status NOT IN ('OPEN', 'CLOSED') OR balance > 41000
GROUP BY QUARTER(opened_date)
ORDER BY opened_quarter DESC;
