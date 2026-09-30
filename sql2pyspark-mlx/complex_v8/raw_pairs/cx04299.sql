WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        LOWER(TRIM(priority)) AS priority_clean,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE agent_id >= 8
      AND requester_id <= 20
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS max_ticket_requester_id
    FROM valid s
),
agent_id_stats AS (
    -- Max requester_id per agent_id
    SELECT agent_id, MAX(requester_id) AS max_requester_id
    FROM summarized_rows
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_requester_id,
        ROUND(s.requester_id - g.max_requester_id, 2) AS requester_id_minus_max
    FROM summarized_rows s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.requester_id = g.max_requester_id
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat_per_requester_id,
        s.max_ticket_requester_id,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor,
        t.event_type
    FROM compared s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.actor = 'system'
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_children_events s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type = 'reopened'
        )
      AND s.priority IN ('P3', 'P1')
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.max_ticket_requester_id,
        s.event_id,
        s.event_at,
        s.event_actor,
        t.id AS event_id_2,
        t.actor AS event_actor_2,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM without_matches s
    JOIN ticket_events t ON t.ticket_id = s.id
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        max_ticket_requester_id,
        event_id,
        event_at,
        event_actor,
        event_id_2,
        event_actor_2,
        event_event_type,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_label,
        CASE WHEN event_actor IN ('customer', 'agent') THEN 'group_a' WHEN event_actor = 'system' THEN 'group_b' ELSE 'other' END AS event_actor_group
    FROM line_level
    WHERE event_actor <> 'agent'
)
-- Final projection
SELECT
    id,
    max_ticket_requester_id,
    event_id,
    event_at,
    event_id_2,
    event_actor_2,
    event_event_type,
    opened_at_label,
    event_actor_group,
    COALESCE(event_event_type, 'UNKNOWN') AS event_event_type_or_unknown
FROM annotated
ORDER BY id
