WITH ranked_sessions AS (
    SELECT
        id,
        customer_id,
        started_at,
        browser,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS pos
    FROM sessions
    WHERE HOUR(started_at) BETWEEN 7 AND 14
)
SELECT
    id,
    customer_id,
    started_at,
    browser
FROM ranked_sessions
WHERE pos <= 1
ORDER BY customer_id DESC;
