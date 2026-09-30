WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(browser, ' ', device) AS browser_device,
        COALESCE(browser, 'DIRECT') AS browser_filled
    FROM sessions
    WHERE browser IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY session_id DESC, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        browser_filled,
        is_new_burst,
        FIRST_VALUE(browser, TRUE) OVER (PARTITION BY browser ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        SUM(session_id) OVER (PARTITION BY country, customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_session_id
    FROM latest
    WHERE is_new_burst <= 1
),
scored_sessions AS (
    -- Running and moving aggregates
    SELECT
        id,
        is_new_burst,
        first_known_browser,
        SUM(is_new_burst) OVER (PARTITION BY device, customer_id ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_new_burst,
        LAG(is_new_burst, 1) OVER w_part AS prev_is_new_burst,
        LAG(is_new_burst, 1, 0) OVER w_part_2 AS prev_is_new_burst_2,
        LAG(is_new_burst, 2, 0) OVER w_grp AS prev_is_new_burst_3,
        AVG(cumulative_session_id) OVER w_running AS cumulative_session_id_avg_3
    FROM scored
    WINDOW w_part AS (PARTITION BY device ORDER BY is_new_burst, id), w_part_2 AS (ORDER BY cumulative_session_id, id), w_grp AS (PARTITION BY customer_id ORDER BY cumulative_session_id, id), w_running AS (PARTITION BY customer_id, device ORDER BY cumulative_session_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.session_id IN (9, 24, 31, 39)
        )
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        is_new_burst,
        first_known_browser,
        cumulative_is_new_burst,
        prev_is_new_burst,
        prev_is_new_burst_2,
        prev_is_new_burst_3,
        cumulative_session_id_avg_3,
        ROUND(cumulative_is_new_burst * 1.1, 2) AS cumulative_is_new_burst_adjusted,
        prev_is_new_burst / NULLIF(cumulative_is_new_burst, 0) AS prev_is_new_burst_per_cumulative_is_new_burst
    FROM eligible
    WHERE cumulative_is_new_burst <= 3
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_is_new_burst_2 >= 1 THEN 'HIGH' ELSE 'LOW' END AS prev_is_new_burst_2_tier,
    CASE WHEN cumulative_is_new_burst_adjusted >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_is_new_burst_adjusted_tier,
    COUNT(*) AS num_rows,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(is_new_burst, 0.9) AS is_new_burst_p90,
    MIN(prev_is_new_burst_2) AS min_prev_is_new_burst_2,
    GROUPING(CASE WHEN prev_is_new_burst_2 >= 1 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM valid
GROUP BY GROUPING SETS ((CASE WHEN prev_is_new_burst_2 >= 1 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN cumulative_is_new_burst_adjusted >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN prev_is_new_burst_2 >= 1 THEN 'HIGH' ELSE 'LOW' END))
