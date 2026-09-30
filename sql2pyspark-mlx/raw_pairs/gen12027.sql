SELECT *, LOWER(browser) AS browser_lower FROM sessions WHERE device = 'mobile' AND (started_at >= '2023-11-01' OR started_at >= '2023-12-01')
