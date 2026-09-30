SELECT
    browser,
    started_at,
    HOUR(started_at) AS started_at_hour,
    LOWER(country) AS country_lower
FROM sessions
WHERE country = 'JP'
