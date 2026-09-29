SELECT t.id, t.txn_type, t.merchant FROM transactions t WHERE t.amount <= 3400 AND t.account_id IN (SELECT id FROM accounts WHERE opened_date BETWEEN '2024-11-01' AND '2025-08-01')
