SELECT id, browser, country, LENGTH(browser) browser_length FROM sessions WHERE HOUR(started_at) BETWEEN 12 AND 15 AND device <> 'desktop'
