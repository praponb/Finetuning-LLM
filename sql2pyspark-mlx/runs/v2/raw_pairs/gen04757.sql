SELECT account_type, customer_id, balance
FROM accounts
WHERE balance BETWEEN 24000 AND 44000 AND opened_date <= '2025-09-09'
ORDER BY balance DESC;
