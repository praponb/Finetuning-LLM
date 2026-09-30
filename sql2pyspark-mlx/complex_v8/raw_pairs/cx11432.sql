WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        ARRAY_CONTAINS(tags, 'bug') AS has_bug
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        has_bug,
        prev_opened_at,
        starts_session,
        burst_id,
        COUNT(*) OVER w_closed_at_year AS rows_in_closed_at_year,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY YEAR(closed_at)), 2) AS requester_id_vs_closed_at_year_avg,
        ROUND(AVG(starts_session) OVER (PARTITION BY has_bug, starts_session ORDER BY closed_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS starts_session_avg_3_prior,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY has_bug, starts_session ORDER BY closed_at DESC, id))) / 60, 1) AS minutes_since_prev_opened_at,
        ROW_NUMBER() OVER (PARTITION BY has_bug, starts_session ORDER BY closed_at DESC, id) AS seq_no,
        SIZE(COLLECT_SET(channel) OVER w_closed_at_year) AS distinct_channel_in_closed_at_year
    FROM sessionized
    WINDOW w_closed_at_year AS (PARTITION BY YEAR(closed_at))
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    has_bug,
    starts_session,
    burst_id,
    rows_in_closed_at_year,
    requester_id_vs_closed_at_year_avg,
    seq_no,
    closed_at + INTERVAL 15 MINUTES AS closed_at_plus_15m,
    seq_no - starts_session AS seq_no_minus_starts_session
FROM windowed
ORDER BY closed_at_plus_15m DESC, burst_id DESC
