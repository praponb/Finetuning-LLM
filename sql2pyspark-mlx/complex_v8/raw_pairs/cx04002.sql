WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 7 AND 9
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_len,
        t.agent_id AS ticket_agent_id,
        t.channel AS ticket_channel
    FROM cleaned_events s
    JOIN tickets t ON t.id = s.ticket_id
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.*,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        t.opened_at AS ticket_opened_at
    FROM combined_details s
    JOIN support_agents su ON su.id = s.ticket_agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_type_len,
        s.ticket_agent_id,
        s.ticket_channel,
        s.agent_team,
        s.agent_hired_on,
        s.agent_tier,
        s.ticket_opened_at,
        t.closed_at AS ticket_closed_at,
        t.agent_id AS ticket_agent_id_2
    FROM with_details_agent s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
filtered AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.ticket_agent_id_2
              AND t.csat BETWEEN 2 AND 4
        )
      AND s.event_type IN ('replied', 'assigned')
),
agent_tier_stats AS (
    -- Avg event_type_len per agent_tier
    SELECT agent_tier, AVG(event_type_len) AS avg_event_type_len
    FROM filtered
    GROUP BY agent_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_type_len,
        ROUND(s.event_type_len - g.avg_event_type_len, 2) AS event_type_len_minus_avg
    FROM filtered s
    JOIN agent_tier_stats g ON g.agent_tier = s.agent_tier
    WHERE s.event_type_len > g.avg_event_type_len
),
combined_details_2 AS (
    -- Attach agent attributes
    SELECT s.*, su.agent_name, t.requester_id AS ticket_requester_id
    FROM compared s
    JOIN support_agents su ON su.id = s.ticket_agent_id
    JOIN tickets t ON t.id = s.ticket_id
    WHERE LENGTH(TRIM(su.agent_name)) >= 6
)
SELECT
    id,
    ticket_id,
    event_type,
    event_type_len,
    ticket_agent_id,
    ticket_channel,
    agent_team,
    agent_hired_on,
    agent_tier,
    ticket_agent_id_2,
    event_type_len_minus_avg,
    agent_name,
    ticket_requester_id,
    QUARTER(agent_hired_on) AS agent_hired_on_quarter
FROM combined_details_2
ORDER BY agent_tier DESC, ticket_id DESC, id
LIMIT 10
