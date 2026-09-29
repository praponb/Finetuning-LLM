WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-24 08:00:00'
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.tags,
        s.days_since_opened_at,
        su.agent_name,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE UPPER(TRIM(su.agent_name)) LIKE 'L%'
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(agent_name)) AS agent_name_len,
        CONCAT(agent_team, '#', CAST(agent_id AS STRING)) AS agent_team_ref,
        ABS(days_since_opened_at - 245) AS days_since_opened_at_distance
    FROM enriched
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.tags,
        s.days_since_opened_at,
        s.agent_tier,
        s.agent_name_len,
        s.agent_team_ref,
        s.days_since_opened_at_distance,
        su.team AS agent_team_2,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier_2
    FROM derived s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(CASE WHEN priority = 'P2' THEN 1 END) AS ticket_priority_count
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count
    FROM joined_agent s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    priority,
    tags,
    days_since_opened_at,
    agent_tier,
    agent_name_len,
    agent_team_ref,
    days_since_opened_at_distance,
    agent_team_2,
    agent_tier_2,
    ticket_priority_count
FROM with_activity
ORDER BY agent_name_len DESC, agent_tier DESC
