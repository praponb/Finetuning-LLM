SELECT *, COALESCE(browser, 'N/A') AS browser_filled, INITCAP(TRIM(browser)) AS browser_title
FROM sessions
WHERE customer_id IS NOT NULL AND device NOT IN ('mobile', 'tablet')
