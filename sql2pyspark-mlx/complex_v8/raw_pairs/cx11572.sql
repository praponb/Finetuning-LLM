WITH filtered AS (
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
        DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND csat < 4
),
with_parent AS (
    -- Join support_agents
    SELECT
        s.*,
        su.agent_name,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
),
deduped AS (
    -- Keep the earliest row per channel
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
        closed_at_label,
        opened_at_label,
        agent_name,
        agent_tier,
        agent_team
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel ORDER BY closed_at, id) AS rn
        FROM with_parent
    ) ranked
    WHERE rn = 1
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_opened_at,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at
    FROM deduped s
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    channel,
    csat,
    tags,
    closed_at_label,
    opened_at_label,
    agent_tier,
    agent_team,
    last_ticket_opened_at,
    TO_DATE(last_ticket_opened_at) AS last_ticket_opened_at_date
FROM profiled
ORDER BY agent_id DESC
