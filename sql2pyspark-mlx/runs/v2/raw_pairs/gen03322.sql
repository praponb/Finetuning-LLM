SELECT
    we.session_id,
    s.country,
    s.customer_id,
    c.credit_limit,
    CASE WHEN s.country IN ('TH', 'UK') THEN 'Priority' ELSE 'Other' END AS country_group
FROM web_events we
FULL OUTER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
ORDER BY customer_id ASC
