SELECT
    id,
    end_date,
    DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
    CASE WHEN status IN ('EXPIRED', 'PAUSED') THEN 'Group A' ELSE 'Other' END AS status_group
FROM subscriptions
WHERE status IN ('EXPIRED', 'ACTIVE') AND plan = 'basic' AND YEAR(end_date) = 2025
ORDER BY end_date
