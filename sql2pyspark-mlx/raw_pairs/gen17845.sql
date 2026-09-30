SELECT
    s.id,
    s.started_at,
    s.customer_id,
    s.country,
    c.age,
    INITCAP(TRIM(c.segment)) AS segment_title
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE s.device NOT IN ('mobile', 'tablet') AND s.country != 'SG'
ORDER BY s.started_at, id
LIMIT 5
