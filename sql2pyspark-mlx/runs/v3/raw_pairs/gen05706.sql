SELECT accounts.id, accounts.balance
FROM accounts
WHERE accounts.opened_date BETWEEN '2024-08-09' AND '2025-07-13' AND accounts.id IN (SELECT account_id FROM transactions)
ORDER BY id DESC;
