WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        ARRAY_CONTAINS(tags, 'refund') AS has_refund
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 9 AND 12
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        s.has_refund,
        s.prev_opened_at,
        s.starts_session,
        s.burst_id,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM sessionized s
    JOIN support_agents su ON su.id = s.agent_id
),
per_group AS (
    -- Summarise per group
    SELECT
        channel,
        TO_DATE(prev_opened_at) AS prev_opened_at_day,
        has_refund,
        ROUND(AVG(burst_id), 2) AS avg_burst_id,
        SUM(starts_session) AS total_starts_session,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(prev_opened_at) AS last_prev_opened_at
    FROM combined_details
    GROUP BY channel, TO_DATE(prev_opened_at), has_refund
    HAVING COUNT(*) >= 2
)
SELECT
    channel,
    prev_opened_at_day,
    has_refund,
    cnt
FROM per_group
ORDER BY prev_opened_at_day DESC NULLS FIRST
