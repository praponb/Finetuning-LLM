WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-25 08:00:00'
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        channel,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, burst_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        channel,
        n_events,
        burst_id,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        COUNT(DISTINCT channel) AS distinct_channels,
        ROUND(STDDEV(n_events), 2) AS stddev_n_events
    FROM sessions
    GROUP BY channel, n_events, burst_id
    HAVING COUNT(*) >= 3
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        distinct_channels - burst_id AS distinct_channels_minus_burst_id,
        ROUND(burst_id / NULLIF(stddev_n_events, 0), 3) AS burst_id_per_stddev_n_events,
        CONCAT(channel, '#', CAST(n_events AS STRING)) AS channel_ref
    FROM summary
    WHERE n_events < 4
)
-- Final projection
SELECT
    channel,
    n_events,
    row_count,
    channel_list,
    distinct_channels,
    distinct_channels_minus_burst_id,
    burst_id_per_stddev_n_events,
    ROUND(row_count / NULLIF(stddev_n_events, 0), 3) AS row_count_per_stddev_n_events
FROM annotated
