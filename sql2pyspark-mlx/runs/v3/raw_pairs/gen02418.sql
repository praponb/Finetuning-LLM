SELECT
    browser,
    customer_id,
    started_at,
    CASE WHEN country IN ('DE', 'UK') THEN 'Group A' ELSE 'Other' END AS country_group
FROM sessions
WHERE HOUR(started_at) BETWEEN 9 AND 15
ORDER BY customer_id
