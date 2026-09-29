SELECT *, UPPER(country) AS country_upper, COALESCE(browser, 'Unknown') AS browser_filled
FROM sessions
WHERE country = 'US';
