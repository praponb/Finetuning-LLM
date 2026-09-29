SELECT browser, CASE WHEN device = 'mobile' THEN 1 ELSE 0 END AS is_mobile FROM sessions WHERE customer_id = 24 ORDER BY is_mobile, browser;
