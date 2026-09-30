SELECT *
FROM accounts
WHERE opened_date BETWEEN '2023-07-03' AND '2025-05-01' AND balance >= 70000 AND customer_id = 26
ORDER BY balance DESC;
