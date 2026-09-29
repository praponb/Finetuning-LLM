WITH valid AS (
    SELECT *, TO_DATE(started_at) AS started_at_date
    FROM sessions
    WHERE started_at > DATE '2023-09-23'
      AND browser IS NOT NULL
),
with_prev AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM valid
),
gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
not_flagged AS (
    SELECT s.*
    FROM sessionized s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.started_at
        )
),
with_dims AS (
    SELECT
        e.*,
        p.city,
        p.segment,
        p.signup_date
    FROM not_flagged e
    INNER JOIN customers p
        ON e.customer_id = p.id
)
SELECT *
FROM (
    SELECT
        city,
        browser,
        session_start_flag
    FROM with_dims
) src
PIVOT (
    AVG(session_start_flag)
    FOR browser IN ('Safari' AS safari, 'Edge' AS edge)
)
