WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM base_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
grouped AS (
    -- Summarise per group
    SELECT
        is_new_session,
        MAX(event_at) AS last_event_at,
        SUM(CASE WHEN actor = 'agent' THEN 1 ELSE 0 END) AS n_actor_agent,
        ROUND(100 * COUNT(CASE WHEN actor IN ('system', 'agent') THEN 1 END) / COUNT(*), 1) AS pct_actor_in_system_agent,
        PERCENTILE_APPROX(burst_id, 0.5) AS median_burst_id,
        MIN(is_new_session) AS min_is_new_session,
        SUM(is_new_session) AS total_is_new_session
    FROM sessionized
    GROUP BY 1
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY is_new_session, median_burst_id) AS rows_in_is_new_session_median_burst_id,
        ROUND(CUME_DIST() OVER w_is_new_session_median_burst_2, 3) AS pct_actor_in_system_agent_cume_dist
    FROM grouped
    WINDOW w_is_new_session_median_burst_2 AS (PARTITION BY is_new_session, median_burst_id ORDER BY pct_actor_in_system_agent DESC, is_new_session), w_is_new_session_median_burst AS (PARTITION BY is_new_session, median_burst_id)
)
-- Subtotals with GROUPING SETS
SELECT
    total_is_new_session,
    median_burst_id,
    min_is_new_session,
    GROUPING(total_is_new_session) AS is_total_total_is_new_session,
    MIN(is_new_session) AS min_is_new_session_2,
    MIN(pct_actor_in_system_agent) AS min_pct_actor_in_system_agent
FROM with_trends
GROUP BY GROUPING SETS ((total_is_new_session, median_burst_id, min_is_new_session), (total_is_new_session), (median_burst_id), ())
