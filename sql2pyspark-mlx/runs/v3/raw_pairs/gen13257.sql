SELECT id, balance, customer_id
FROM accounts
WHERE balance > (SELECT AVG(balance) FROM accounts WHERE opened_date <= '2023-06-24')
ORDER BY balance DESC;
