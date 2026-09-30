SELECT started_at, LOWER(browser) AS browser_lower
FROM sessions
WHERE country = 'US';
