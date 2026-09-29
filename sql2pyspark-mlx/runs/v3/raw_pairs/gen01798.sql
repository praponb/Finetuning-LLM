SELECT
    s.browser,
    s.country,
    c.segment,
    LOWER(s.country) AS country_lower
FROM sessions s
FULL OUTER JOIN customers c ON c.id = s.customer_id
ORDER BY segment ASC, browser ASC;
