WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        TO_DATE(started_at) AS started_at_date
    FROM sessions
    WHERE customer_id IN (31, 39)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM sessions_sessions s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM not_flagged
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device, customer_id ORDER BY is_new_burst, id) AS seq_no,
        FIRST_VALUE(is_new_burst, TRUE) OVER (PARTITION BY device ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_new_burst,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY is_new_burst DESC, id) AS seq_no_2,
        MAX(is_new_burst) OVER (PARTITION BY device) AS device_max_is_new_burst
    FROM islands
),
with_dims AS (
    -- Attach customers attributes
    SELECT e.*, p.segment
    FROM enriched e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY device ORDER BY session_start_flag) AS latest_rank
    FROM with_dims
    QUALIFY latest_rank = 1
)
-- Ranking and gap features
SELECT
    *,
    CUME_DIST() OVER (PARTITION BY browser ORDER BY device_max_is_new_burst DESC NULLS LAST) AS device_max_is_new_burst_cume_dist,
    SUM(burst_id) OVER (PARTITION BY browser ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_burst_id,
    ROW_NUMBER() OVER (PARTITION BY segment ORDER BY session_start_flag DESC, id) AS seq_no_3
FROM latest;
