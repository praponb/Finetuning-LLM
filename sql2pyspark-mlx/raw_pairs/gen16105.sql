SELECT id, balance, ROUND(balance * 1.1, 2) AS balance_scaled
FROM accounts
WHERE opened_date >= '2025-07-01' AND balance <= 40000 AND status IN ('FROZEN', 'OPEN');
