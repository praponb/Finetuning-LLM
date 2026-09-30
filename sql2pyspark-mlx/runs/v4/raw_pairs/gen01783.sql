SELECT SUM(CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END) AS frozen_count, MAX(opened_date) AS last_opened_date
FROM accounts
WHERE opened_date > '2024-12-01' OR opened_date >= '2024-04-01'
ORDER BY frozen_count DESC
