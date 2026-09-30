SELECT
    id,
    device,
    TRIM(browser) AS clean_browser,
    LENGTH(browser) AS browser_length
FROM sessions
WHERE country = 'TH' AND (device = 'mobile' OR country = 'TH')
