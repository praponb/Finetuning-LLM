SELECT id, browser, COALESCE(browser, 'N/A') AS browser_filled
FROM sessions
WHERE device IN ('mobile', 'desktop', 'tablet')
ORDER BY browser_filled DESC;
