SELECT started_at, CONCAT(browser, ' - ', device) AS browser_device FROM sessions WHERE device = 'desktop' AND country IN ('US', 'TH');
