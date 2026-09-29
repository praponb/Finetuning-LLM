SELECT SUM(CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END) AS frozen_count
FROM accounts
ORDER BY frozen_count DESC;
