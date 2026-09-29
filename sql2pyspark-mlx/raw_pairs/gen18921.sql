SELECT CASE WHEN budget >= 3504000 THEN 'high' WHEN budget >= 1395000 THEN 'mid' ELSE 'low' END budget_bucket, location, SUM(CASE WHEN location = 'Tokyo' THEN 1 ELSE 0 END) tokyo_count
FROM departments
GROUP BY 1, 2
HAVING tokyo_count < 65
ORDER BY budget_bucket DESC, location ASC
LIMIT 10
