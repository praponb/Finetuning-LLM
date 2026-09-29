WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        DATE_TRUNC('DAY', opened_at) AS opened_at_day,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id
    FROM tickets
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat,
        s.opened_at_day,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM scoped_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier NOT IN ('T3', 'T2')
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at_2,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM joined_agent s
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        agent_tier,
        first_event_at,
        last_event_at,
        LOWER(REGEXP_REPLACE(TRIM(agent_tier), ' ', '_')) AS agent_tier_slug
    FROM with_stats
    WHERE agent_id <= 5
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.first_event_at,
        s.agent_tier_slug,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at,
        t.event_type
    FROM enriched s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        first_event_at,
        event_id,
        event_actor,
        event_type,
        ROUND(PERCENT_RANK() OVER w_event_id_event_actor_by_even, 3) AS event_at_percent_rank,
        ROW_NUMBER() OVER w_event_id_event_actor_by_even_2 AS seq_no,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY event_id, event_actor), 2) AS requester_id_vs_event_id_event_actor_avg
    FROM detail_rows_events
    WINDOW w_event_id_event_actor_by_even AS (PARTITION BY event_id, event_actor ORDER BY event_at), w_event_id_event_actor_by_even_2 AS (PARTITION BY event_id, event_actor ORDER BY event_at, id, event_id)
),
enriched_event AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.first_event_at,
        s.event_id,
        s.event_type,
        s.event_at_percent_rank,
        s.seq_no,
        s.requester_id_vs_event_id_event_actor_avg,
        t.ticket_id AS event_ticket_id,
        t.event_at,
        ti.opened_at AS ticket_opened_at
    FROM windowed s
    JOIN ticket_events t ON t.id = s.event_id
    LEFT JOIN tickets ti ON ti.id = t.ticket_id
    WHERE t.ticket_id >= 55
)
-- Subtotals with CUBE
SELECT
    event_ticket_id,
    agent_id,
    seq_no,
    ROUND(SUM(requester_id_vs_event_id_event_actor_avg), 2) AS total_requester_id_vs_event_id_event_actor_avg
FROM enriched_event
GROUP BY CUBE(event_ticket_id, agent_id, seq_no)
ORDER BY event_ticket_id NULLS LAST, agent_id NULLS LAST, seq_no NULLS LAST
