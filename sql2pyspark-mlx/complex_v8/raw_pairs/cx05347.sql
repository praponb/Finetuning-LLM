WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE event_type = 'replied'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY event_at, id) AS seq_no,
        LEAD(event_at) OVER (ORDER BY event_at, id) AS next_event_at,
        LAST_VALUE(event_at) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM base
),
totals AS (
    -- Group-level totals
    SELECT
        ticket_id,
        YEAR(next_event_at) AS next_event_at_year,
        SUM(CASE WHEN actor NOT IN ('agent', 'system') THEN 1 ELSE 0 END) AS n_actor_not_in_agent_system,
        SUM(seq_no) FILTER (WHERE actor <> 'system') AS seq_no_actor_system
    FROM sequenced
    WHERE event_type IN ('assigned', 'replied')
    GROUP BY ticket_id, YEAR(next_event_at)
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM totals s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at < TIMESTAMP '2025-02-18 12:00:00'
              AND t.actor IN ('system', 'customer')
        )
      AND s.seq_no_actor_system <= 38
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.ticket_id,
        s.next_event_at_year,
        s.seq_no_actor_system,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat,
        su.agent_name,
        su.team AS agent_team
    FROM without_matches s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.requester_id >= 7
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) FILTER (WHERE actor = 'agent') AS event_actor_count,
        MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        t.first_event_at
    FROM enriched s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
unique_rows AS (
    -- Keep the latest row per agent_team
    SELECT
        ticket_id,
        next_event_at_year,
        seq_no_actor_system,
        ticket_requester_id,
        ticket_csat,
        agent_name,
        agent_team,
        event_actor_count,
        first_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_team ORDER BY event_actor_count DESC, ticket_id DESC, next_event_at_year DESC) AS rn
        FROM with_stats
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    ticket_id,
    next_event_at_year,
    seq_no_actor_system,
    ticket_requester_id,
    ticket_csat,
    agent_name,
    agent_team,
    event_actor_count,
    first_event_at
FROM unique_rows
WHERE agent_team = 'Tech'
