SELECT *
FROM (
    SELECT status, TO_DATE(paid_at) AS paid_day, ROUND((100 * SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cash
    FROM payments
    WHERE status = 'FAILED'
    GROUP BY status, TO_DATE(paid_at)
) AS x
WHERE pct_cash < 69
ORDER BY status ASC
