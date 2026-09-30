SELECT
    id,
    end_date,
    QUARTER(start_date) AS start_date_quarter,
    DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
FROM subscriptions
WHERE status IN ('PAUSED', 'EXPIRED', 'ACTIVE') AND end_date < '2025-08-01';
