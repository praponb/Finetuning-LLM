SELECT opened_date
FROM accounts
WHERE status <> 'OPEN' AND opened_date BETWEEN '2023-07-01' AND '2025-05-01' AND MONTH(opened_date) IN (5, 9, 11)
