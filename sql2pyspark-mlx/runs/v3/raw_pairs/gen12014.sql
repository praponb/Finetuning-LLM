SELECT status, customer_id
FROM accounts
WHERE customer_id IN (6, 21, 32) AND opened_date >= '2024-03-01';
