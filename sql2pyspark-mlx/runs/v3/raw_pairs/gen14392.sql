SELECT id, device, browser, started_at, SUBSTRING(country, 1, 2) AS country_prefix, CASE WHEN browser IN ('Chrome', 'Safari') THEN 'Priority' ELSE 'Other' END AS browser_group FROM sessions
