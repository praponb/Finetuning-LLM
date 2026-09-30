SELECT
    id,
    browser,
    DATE_FORMAT(started_at, 'yyyy-MM-dd') AS started_at_label,
    DATEDIFF(CURRENT_DATE(), started_at) / 365 AS years_since_started_at
FROM sessions
WHERE device <> 'tablet' OR TO_DATE(started_at) >= '2025-08-28';
