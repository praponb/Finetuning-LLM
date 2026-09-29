WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        tags
    FROM tickets
    WHERE requester_id > 4
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT COUNT(*) FILTER (WHERE t.event_type <> 'created') FROM ticket_events t WHERE t.ticket_id = s.id) AS event_event_type_count
    FROM recent s
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.priority = 'P3'
        )
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.tags,
        s.first_event_at,
        s.event_count,
        s.event_event_type_count,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM filtered s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(event_count, event_event_type_count) AS least_event_count_event_event_type_count,
        SIZE(tags) AS tags_count,
        LENGTH(TRIM(agent_team)) AS agent_team_len
    FROM combined_details_agent
    WHERE opened_at >= TIMESTAMP '2025-02-06 06:00:00'
)
SELECT DISTINCT event_event_type_count, agent_team_len, requester_id
FROM enriched
WHERE agent_team <> 'VIP'
ORDER BY requester_id, event_event_type_count DESC
