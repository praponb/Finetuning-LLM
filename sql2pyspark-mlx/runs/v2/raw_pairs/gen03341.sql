SELECT id, customer_id, status
FROM accounts
WHERE account_type = 'SAVINGS'
INTERSECT
SELECT id, customer_id, status
FROM accounts
WHERE opened_date <= '2024-08-01';
