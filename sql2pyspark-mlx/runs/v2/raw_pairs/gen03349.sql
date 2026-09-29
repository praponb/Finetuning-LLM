SELECT id, country, started_at, browser, FIRST_VALUE(browser) OVER (PARTITION BY country ORDER BY started_at, id) AS first_browser FROM sessions WHERE country = 'UK'
