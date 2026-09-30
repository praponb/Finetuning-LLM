SELECT
    started_at,
    browser,
    device,
    DATE_FORMAT(started_at, 'MMM yyyy') AS started_at_label
FROM sessions
WHERE device IN ('desktop', 'tablet', 'mobile') AND country IN ('DE', 'JP') AND browser = 'Safari';
