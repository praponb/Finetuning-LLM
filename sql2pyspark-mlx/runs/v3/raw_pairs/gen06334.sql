SELECT t.id, t.merchant, t.account_id, t.amount FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE opened_date < '2025-09-24') ORDER BY t.account_id DESC, id ASC LIMIT 50;
