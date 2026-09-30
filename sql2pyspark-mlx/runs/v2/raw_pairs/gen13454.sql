SELECT
    c.segment,
    c.city,
    MIN(s.started_at) AS min_started_at,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.browser))) AS browser_values
FROM sessions s
JOIN customers c ON c.id = s.customer_id
WHERE c.name LIKE '%Pim%'
GROUP BY c.segment, c.city;
