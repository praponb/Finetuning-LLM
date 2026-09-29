SELECT
    c.is_active,
    MONTH(c.signup_date) AS signup_month,
    SORT_ARRAY(COLLECT_SET(s.device)) AS device_list,
    MIN(s.started_at) AS min_started_at
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY c.is_active, MONTH(c.signup_date)
ORDER BY signup_month, min_started_at;
