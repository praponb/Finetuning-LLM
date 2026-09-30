SELECT opened_date
FROM accounts
WHERE MONTH(opened_date) IN (3, 6, 11) AND opened_date > '2025-04-06'
ORDER BY opened_date DESC
