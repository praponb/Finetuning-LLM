SELECT id, opened_date
FROM accounts
WHERE customer_id IS NULL
INTERSECT
SELECT id, opened_date
FROM accounts
WHERE opened_date BETWEEN '2024-11-01' AND '2025-06-11';
