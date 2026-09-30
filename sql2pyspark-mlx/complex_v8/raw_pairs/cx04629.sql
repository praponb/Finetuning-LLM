WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE csat >= 3
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        channel,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, session_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_channel_by_session_end_ties AS session_end_rank,
        ROUND(n_events - AVG(n_events) OVER (PARTITION BY YEAR(session_end)), 2) AS n_events_vs_session_end_year_avg,
        NTH_VALUE(session_id, 2) OVER (ORDER BY session_start, channel, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_id
    FROM sessions
    WINDOW w_channel_by_session_end_ties AS (PARTITION BY channel ORDER BY session_end DESC)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(session_start) OVER (PARTITION BY YEAR(session_start) ORDER BY session_end_rank, channel, session_id) AS prev_session_start,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', session_end) ORDER BY duration_min ASC), 3) AS duration_min_cume_dist,
        DENSE_RANK() OVER (PARTITION BY YEAR(session_start) ORDER BY session_end_rank) AS session_end_rank_dense_rank,
        session_id - LAG(session_id) OVER (PARTITION BY DATE_TRUNC('MONTH', session_end) ORDER BY duration_min ASC, channel, session_id) AS session_id_change,
        ROUND(PERCENT_RANK() OVER w_all_by_n_events_vs_session_e, 3) AS n_events_vs_session_end_year_avg_percent_rank
    FROM with_windows
    WINDOW w_all_by_n_events_vs_session_e AS (ORDER BY n_events_vs_session_end_year_avg DESC)
)
-- Final projection
SELECT
    channel,
    session_start,
    session_end,
    n_events,
    session_csat,
    session_end_rank,
    n_events_vs_session_end_year_avg,
    second_session_id,
    prev_session_start,
    duration_min_cume_dist,
    session_end_rank_dense_rank,
    DATE_TRUNC('HOUR', session_end) AS session_end_hour,
    CASE WHEN channel IN ('phone', 'chat') THEN 'group_a' WHEN channel = 'email' THEN 'group_b' ELSE 'other' END AS channel_group
FROM sequenced
ORDER BY session_csat DESC NULLS FIRST, session_end DESC
