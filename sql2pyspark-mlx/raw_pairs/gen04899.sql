SELECT browser, customer_id, LENGTH(browser) AS browser_length, DATE_TRUNC('week', started_at) AS started_at_period FROM sessions WHERE country = 'JP' ORDER BY browser
