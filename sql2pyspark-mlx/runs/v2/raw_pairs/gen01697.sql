SELECT *
FROM (
    SELECT QUARTER(end_date) end_quarter, COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) active_count
    FROM subscriptions
    GROUP BY QUARTER(end_date)
) sub
WHERE active_count >= 6
ORDER BY active_count DESC
