SELECT *, SUBSTRING(browser, 1, 2) AS browser_prefix FROM sessions WHERE device <> 'mobile' ORDER BY started_at
