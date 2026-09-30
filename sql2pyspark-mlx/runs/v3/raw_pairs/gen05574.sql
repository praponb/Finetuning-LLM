SELECT country, UPPER(browser) AS browser_upper, COALESCE(browser, 'Unknown') AS browser_filled FROM sessions WHERE customer_id IN (5, 7, 35)
