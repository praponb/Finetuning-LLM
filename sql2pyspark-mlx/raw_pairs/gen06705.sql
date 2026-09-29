WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(browser, TRUE) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_browser,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) event_seq,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY customer_id, device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_country
    FROM sessions
    WHERE TO_DATE(started_at) >= '2025-03-01'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.browser,
        a.country,
        a.row_seq,
        a.event_seq,
        a.first_known_country,
        a.is_new_session,
        a.session_no,
        p.country AS customer_country,
        p.credit_limit
    FROM islands a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < '2023-11-21'
        )
)
-- Aggregate with conditional metrics
SELECT
    browser,
    CASE WHEN credit_limit >= 17700 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    customer_country,
    APPROX_PERCENTILE(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    ROUND(AVG(is_new_session), 2) AS avg_is_new_session,
    SUM(is_new_session) AS total_is_new_session,
    DENSE_RANK() OVER (PARTITION BY browser ORDER BY ROUND(AVG(is_new_session), 2) DESC) AS avg_is_new_session_dense_rank
FROM not_flagged
GROUP BY browser, CASE WHEN credit_limit >= 17700 THEN 'LARGE' ELSE 'SMALL' END, customer_country
HAVING avg_is_new_session >= 0
ORDER BY total_is_new_session DESC;
