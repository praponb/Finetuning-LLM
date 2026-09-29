WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE priority = 'P4'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        QUARTER(closed_at) AS closed_at_quarter,
        LENGTH(TRIM(priority)) AS priority_len_2
    FROM base
    WHERE ARRAY_CONTAINS(tags, 'outage')
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM tagged s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.priority_len,
        s.priority_len_2,
        s.ticket_count,
        su.team AS agent_team,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM with_counts s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    requester_id,
    priority,
    csat,
    tags,
    priority_len,
    priority_len_2,
    ticket_count,
    agent_team,
    agent_name,
    agent_hired_on
FROM joined
ORDER BY priority_len_2 DESC
