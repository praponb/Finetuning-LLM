WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE end_date > '2025-07-08' AND status IN ('ACTIVE', 'EXPIRED')
),
summary AS (
    SELECT status, ROUND((100 * SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_active, SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count
    FROM src
    GROUP BY status
)
SELECT status, pct_active
FROM summary
WHERE cancelled_count < 76;
