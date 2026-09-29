SELECT browser, started_at, SUBSTRING(browser, 1, 4) AS browser_prefix, CASE WHEN browser IN ('Edge', 'Firefox') THEN 'Priority' ELSE 'Other' END AS browser_group FROM sessions ORDER BY started_at;
