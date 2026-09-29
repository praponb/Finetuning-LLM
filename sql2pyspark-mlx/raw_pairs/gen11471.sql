SELECT id, account_type, FLOOR(balance) AS balance_whole
FROM accounts
WHERE opened_date > '2024-07-24' AND (customer_id IS NULL OR balance <= 18000);
