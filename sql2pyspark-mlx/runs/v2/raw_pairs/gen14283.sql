SELECT status FROM accounts WHERE opened_date < '2025-06-01' UNION ALL SELECT status FROM payments;
