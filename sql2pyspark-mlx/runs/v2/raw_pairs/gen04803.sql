SELECT *, CASE WHEN country IN ('US', 'DE') THEN 'Group A' ELSE 'Other' END AS country_group
FROM customers
WHERE is_active AND country NOT IN ('UK', 'DE') AND email NOT LIKE '%@GMAIL.COM'
