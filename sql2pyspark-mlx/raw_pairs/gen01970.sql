SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count
FROM accounts
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
ORDER BY frozen_count DESC, opened_month
LIMIT 3
