WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per channel
    SELECT *, LAG(closed_at) OVER (PARTITION BY channel ORDER BY closed_at, id) AS prev_closed_at
    FROM base_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        channel,
        session_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, session_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        channel,
        session_id,
        session_start,
        n_events,
        session_csat,
        duration_min,
        COUNT(*) OVER (PARTITION BY TO_DATE(session_start), channel) AS rows_in_session_start_to_date_channel,
        ROUND(duration_min / NULLIF(SUM(duration_min) OVER w_session_start_to_date_channe, 0), 4) AS duration_min_share_of_session_start_to_date_chan,
        ROUND(n_events / NULLIF(SUM(n_events) OVER (PARTITION BY TO_DATE(session_start), channel), 0), 4) AS n_events_share_of_session_start_to_date_channel,
        SUM(session_id) OVER (PARTITION BY TO_DATE(session_start), channel) AS session_start_to_date_channel_sum_session_id,
        ROUND(CUME_DIST() OVER (ORDER BY session_csat), 3) AS session_csat_cume_dist
    FROM per_session
    WINDOW w_session_start_to_date_channe AS (PARTITION BY TO_DATE(session_start), channel)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN channel IN ('phone', 'email') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix
    FROM with_trends
    WHERE session_csat > 3
)
-- Final projection
SELECT DISTINCT session_start_to_date_channel_sum_session_id, session_id, duration_min
FROM enriched
ORDER BY duration_min DESC NULLS FIRST
