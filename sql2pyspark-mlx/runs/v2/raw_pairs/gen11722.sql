SELECT opened_date
FROM accounts
WHERE balance <= 22000
UNION ALL
SELECT opened_date
FROM accounts
WHERE opened_date >= '2023-07-01'
ORDER BY opened_date;
