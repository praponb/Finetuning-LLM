SELECT
    c.segment,
    c.is_active,
    COUNT(DISTINCT s.country) AS unique_countrys,
    SUM(CASE WHEN s.browser = 'Firefox' THEN 1 ELSE 0 END) AS firefox_count
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
GROUP BY c.segment, c.is_active
HAVING firefox_count >= 68
ORDER BY firefox_count ASC, segment ASC, is_active ASC
LIMIT 3;
