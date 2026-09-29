SELECT c.segment, SORT_ARRAY(COLLECT_SET(s.country)) AS country_list, COUNT(DISTINCT s.browser) AS unique_browsers
FROM sessions s
JOIN customers c ON c.id = s.customer_id
WHERE s.device = 'mobile'
GROUP BY c.segment
ORDER BY unique_browsers DESC
