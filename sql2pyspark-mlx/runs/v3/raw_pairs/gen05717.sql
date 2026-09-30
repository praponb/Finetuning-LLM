SELECT browser, COALESCE(browser, 'N/A') AS browser_filled, CASE WHEN device = 'tablet' THEN 1 ELSE 0 END AS is_tablet
FROM sessions
WHERE customer_id = 25 AND browser != 'Safari'
