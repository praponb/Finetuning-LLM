WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 12:00:00'
      AND actor IS NOT NULL
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.agent_id AS ticket_agent_id,
        su.tier AS agent_tier
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id AND NOT ARRAY_CONTAINS(t.tags, 'login')
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_agent_id,
        agent_tier,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LEAD(event_at, 3) OVER (PARTITION BY event_type, actor ORDER BY event_at, id) AS next3_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at ASC), 3) AS event_at_percent_rank,
        LAG(event_at) OVER w_event_type_actor_by_event_at_2 AS prev_event_at,
        LEAD(event_at, 2) OVER (ORDER BY event_at DESC, id) AS next2_event_at,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS event_at_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id), 3) AS event_at_percent_rank_2
    FROM joined
    WINDOW w_event_type_actor_by_event_at_2 AS (PARTITION BY event_type, actor ORDER BY event_at, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_agent_tier AS rows_in_agent_tier,
        LAST_VALUE(next3_event_at) OVER (ORDER BY prev_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_next3_event_at,
        COUNT(*) OVER (PARTITION BY MONTH(first_event_at), DATE_TRUNC('MONTH', next2_event_at)) AS rows_in_first_event_at_month_next2_event_at_mont,
        LAG(first_known_event_at, 2) OVER (PARTITION BY TO_DATE(next3_event_at) ORDER BY event_at_percent_rank_2 ASC, id) AS prev2_first_known_event_at,
        MAX(event_at_percent_rank) OVER (PARTITION BY agent_tier ORDER BY first_known_event_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS event_at_percent_rank_max_7,
        SIZE(COLLECT_SET(ticket_agent_id) OVER (PARTITION BY agent_tier)) AS distinct_ticket_agent_id_in_agent_tier
    FROM sequenced
    WINDOW w_agent_tier AS (PARTITION BY agent_tier)
)
SELECT
    id,
    actor,
    ticket_agent_id,
    agent_tier,
    first_known_event_at,
    next3_event_at,
    event_at_percent_rank,
    next2_event_at,
    event_at_percent_rank_2,
    rows_in_agent_tier,
    last_next3_event_at,
    rows_in_first_event_at_month_next2_event_at_mont,
    event_at_percent_rank_max_7
FROM with_trends
ORDER BY first_known_event_at DESC NULLS FIRST, last_next3_event_at DESC
