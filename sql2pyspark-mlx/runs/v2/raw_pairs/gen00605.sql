SELECT status, COUNT(balance) balance_count FROM accounts WHERE balance <= 36000 AND balance >= 58000 AND customer_id IS NULL GROUP BY status ORDER BY balance_count DESC;
