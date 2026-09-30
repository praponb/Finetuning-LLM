SELECT id, balance
FROM accounts
WHERE balance > (SELECT AVG(balance) FROM accounts WHERE opened_date <= '2025-10-31')
