SELECT id, opened_date, status
FROM accounts
WHERE status IS NOT NULL
UNION ALL
SELECT id, opened_date, status
FROM accounts
WHERE opened_date <= '2023-11-01'
ORDER BY opened_date
