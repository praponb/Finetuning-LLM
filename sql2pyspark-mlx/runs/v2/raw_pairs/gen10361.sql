SELECT *
FROM (
    SELECT MONTH(paid_at) AS paid_month, status, ROUND((100 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_success
    FROM payments
    GROUP BY MONTH(paid_at), status
) AS s
WHERE pct_success >= 34;
