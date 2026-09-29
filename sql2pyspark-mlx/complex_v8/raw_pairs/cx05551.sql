WITH cleaned_tickets AS (
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
        CASE WHEN requester_id >= 17 THEN 'HIGH' WHEN requester_id >= 8 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per requester_id_tier
    SELECT *, LAG(opened_at) OVER (PARTITION BY requester_id_tier ORDER BY opened_at, id) AS prev_opened_at
    FROM cleaned_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY requester_id_tier ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        requester_id_tier,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY requester_id_tier, session_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(requester_id_tier), 1, 2) AS requester_id_tier_prefix,
        DATE_TRUNC('HOUR', session_end) AS session_end_hour
    FROM per_session
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(event_count) IGNORE NULLS OVER (ORDER BY session_end DESC NULLS FIRST, requester_id_tier, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_count,
        LAG(session_end_hour) OVER (ORDER BY session_end DESC NULLS FIRST, requester_id_tier, session_id) AS prev_session_end_hour
    FROM labeled
)
-- Final projection
SELECT
    requester_id_tier,
    session_id,
    session_start,
    session_end,
    event_count,
    duration_min,
    requester_id_tier_prefix,
    session_end_hour,
    prev_session_end_hour,
    COALESCE(requester_id_tier, 'UNKNOWN') AS requester_id_tier_or_unknown
FROM ranked
