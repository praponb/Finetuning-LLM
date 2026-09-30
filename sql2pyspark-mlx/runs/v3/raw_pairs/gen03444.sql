SELECT t.id, t.merchant FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE opened_date BETWEEN '2023-03-18' AND '2024-01-01') ORDER BY t.merchant
