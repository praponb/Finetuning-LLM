WITH scoped AS (
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
        opened_at + INTERVAL 30 MINUTES AS opened_at_plus_30m
    FROM tickets
    WHERE priority <> 'P1'
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        burst_id,
        ROUND(AVG(requester_id) OVER (PARTITION BY priority, csat ORDER BY prev_opened_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_4,
        MAX(burst_id) OVER (PARTITION BY csat) AS csat_max_burst_id
    FROM sessionized
)
SELECT
    id,
    requester_id,
    closed_at,
    channel,
    csat,
    tags,
    burst_id,
    requester_id_avg_4,
    csat_max_burst_id,
    DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at,
    LENGTH(TRIM(channel)) AS channel_len
FROM sequenced
WHERE burst_id < 5
