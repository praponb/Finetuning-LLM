WITH scoped_tickets AS (
    -- Scope the tickets rows
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
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE requester_id >= 2
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped_tickets
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
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.channel_len,
        s.burst_id,
        su.tier AS agent_tier,
        su.team AS agent_team,
        su.agent_name
    FROM sessionized s
    JOIN support_agents su ON su.id = s.agent_id AND LENGTH(TRIM(su.agent_name)) >= 5
),
grouped AS (
    -- Group-level totals
    SELECT
        channel,
        csat,
        agent_team,
        MIN(csat) AS min_csat,
        MAX(csat) AS max_csat,
        COUNT(*) AS row_count,
        ROUND(STDDEV(burst_id), 2) AS stddev_burst_id,
        SUM(requester_id) AS total_requester_id
    FROM with_details_agent
    GROUP BY channel, csat, agent_team
)
SELECT
    channel,
    csat,
    agent_team,
    max_csat,
    row_count,
    stddev_burst_id,
    total_requester_id,
    COALESCE(agent_team, 'UNKNOWN') AS agent_team_or_unknown
FROM grouped
ORDER BY csat
