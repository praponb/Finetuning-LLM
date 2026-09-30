SELECT *, TO_DATE(started_at) AS started_at_date, DATE_TRUNC('year', started_at) AS started_at_period
FROM sessions
WHERE device IN ('desktop', 'tablet', 'mobile') AND browser IS NOT NULL
