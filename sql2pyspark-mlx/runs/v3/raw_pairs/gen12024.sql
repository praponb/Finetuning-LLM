SELECT
    id,
    browser,
    device,
    LOWER(browser) AS browser_lower
FROM sessions
ORDER BY browser, id
LIMIT 20;
