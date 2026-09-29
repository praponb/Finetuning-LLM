SELECT
    c.id,
    c.credit_limit,
    c.signup_date,
    t.min_started_at,
    t.country_values
FROM customers c
INNER JOIN (SELECT customer_id, MIN(started_at) AS min_started_at, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
    FROM sessions
    GROUP BY customer_id) t ON c.id = t.customer_id
ORDER BY id DESC;
