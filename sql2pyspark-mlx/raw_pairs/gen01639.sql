WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        browser,
        country,
        CONCAT_WS('/', device, browser) AS device_browser_key,
        SUBSTRING(device, 1, 4) AS device_prefix
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-07-31 09:00:00'
      AND browser = 'Chrome'
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.email,
        c.signup_date
    FROM base s
    LEFT JOIN customers c
        ON s.customer_id = c.id
    WHERE c.city != 'Singapore'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.signup_date
        )
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        device_browser_key,
        signup_date
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id ASC) = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    COUNT(*) AS num_rows,
    COUNT(*) AS num_rows_2,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING(DATE_TRUNC('MONTH', signup_date)) AS gid
FROM ranked
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', signup_date), DATE_TRUNC('YEAR', signup_date)), (DATE_TRUNC('MONTH', signup_date)))
ORDER BY num_rows DESC
