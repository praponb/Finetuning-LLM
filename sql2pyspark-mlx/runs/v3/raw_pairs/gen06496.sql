SELECT id, status FROM accounts WHERE customer_id IN (7, 19, 24, 39) UNION SELECT id, status FROM accounts WHERE opened_date BETWEEN '2024-12-06' AND '2025-03-01' ORDER BY status DESC
