SELECT
    id,
    browser,
    country,
    UPPER(country) AS country_upper
FROM sessions
WHERE country IN ('US', 'DE')
