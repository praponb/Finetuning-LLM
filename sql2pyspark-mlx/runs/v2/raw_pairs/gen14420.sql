SELECT opened_date
FROM accounts
WHERE balance < 75000
UNION ALL
SELECT opened_date
FROM accounts
WHERE customer_id = 3
ORDER BY opened_date DESC
