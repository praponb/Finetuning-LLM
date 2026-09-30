WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 13:00:00'
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_actor_label,
        t.csat AS ticket_csat,
        t.agent_id AS ticket_agent_id,
        t.priority AS ticket_priority
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.agent_id >= 4
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM joined s
    WHERE s.ticket_agent_id IN (
            SELECT t.agent_id
            FROM tickets t
        )
      AND s.ticket_priority = 'P3'
),
joined_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.hired_on AS agent_hired_on
    FROM screened s
    LEFT JOIN support_agents su ON su.id = s.ticket_agent_id AND su.hired_on BETWEEN DATE '2024-05-02' AND DATE '2025-03-30'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(ticket_agent_id) OVER w_actor_event_type) AS distinct_ticket_agent_id_in_actor_event_type,
        RANK() OVER w_ticket_id_by_agent_hired_on AS agent_hired_on_rank,
        LEAD(ticket_csat, 2) OVER w_ticket_id_by_agent_hired_on AS next2_ticket_csat
    FROM joined_agent
    WINDOW w_actor_event_type AS (PARTITION BY actor, event_type), w_ticket_id_by_agent_hired_on AS (PARTITION BY ticket_id ORDER BY agent_hired_on DESC, id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        agent_hired_on_rank,
        next2_ticket_csat,
        ROUND(AVG(agent_hired_on_rank), 2) AS avg_agent_hired_on_rank,
        MIN(event_at) AS first_event_at
    FROM windowed
    WHERE ticket_csat BETWEEN 1 AND 5
    GROUP BY 1, 2
    HAVING MAX(agent_hired_on_rank) > 5
),
screened_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE agent_hired_on_rank > (SELECT AVG(agent_hired_on_rank) FROM rollup_base)
)
-- Subtotals with ROLLUP
SELECT
    next2_ticket_csat,
    agent_hired_on_rank,
    GROUPING(next2_ticket_csat) AS is_total_next2_ticket_csat,
    COUNT(*) AS cnt,
    MAX(first_event_at) AS last_first_event_at
FROM screened_2
GROUP BY ROLLUP(next2_ticket_csat, agent_hired_on_rank)
ORDER BY next2_ticket_csat NULLS LAST, agent_hired_on_rank NULLS LAST
