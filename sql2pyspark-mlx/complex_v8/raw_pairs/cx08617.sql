WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'system'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.channel AS ticket_channel,
        t.tags AS ticket_tags,
        su.agent_name
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id AND t.requester_id <= 18
    JOIN support_agents su ON su.id = t.agent_id
    WHERE t.channel = 'email'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        LEAD(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS next_event_at,
        NTILE(3) OVER (PARTITION BY event_type, actor ORDER BY event_at, id) AS event_at_tercile,
        LAG(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id) AS prev_event_at,
        LEAD(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at, id) AS next_event_at_2,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id) AS seq_no,
        NTILE(5) OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id) AS event_at_quintile,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, actor ORDER BY event_at, id), 3) AS event_at_cume_dist,
        RANK() OVER (PARTITION BY actor ORDER BY event_at) AS event_at_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC), 3) AS event_at_percent_rank
    FROM enriched
    WINDOW w_event_type_actor_by_event_at_2 AS (PARTITION BY event_type, actor ORDER BY event_at, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', first_event_at) AS days_since_first_event_at,
        INITCAP(TRIM(event_type)) AS event_type_clean
    FROM ranked
    WHERE ticket_id <= 29
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    ticket_channel,
    ticket_tags,
    agent_name,
    second_event_at,
    minutes_since_prev_event_at,
    next_event_at,
    event_at_tercile,
    next_event_at_2,
    seq_no,
    event_at_quintile,
    first_known_event_at,
    event_at_cume_dist,
    event_at_rank,
    event_at_percent_rank,
    days_since_first_event_at,
    event_type_clean,
    SIZE(ticket_tags) AS ticket_tags_count,
    event_at_percent_rank - seq_no AS event_at_percent_rank_minus_seq_no
FROM labeled
ORDER BY ticket_id DESC, event_at_cume_dist DESC, id
LIMIT 10
