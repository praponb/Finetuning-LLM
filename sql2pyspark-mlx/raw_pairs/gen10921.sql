SELECT *, UPPER(browser) AS browser_upper, LOWER(browser) AS browser_lower
FROM sessions
WHERE browser IS NULL AND customer_id = 40
