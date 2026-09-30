WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM ticket_events
    WHERE actor = 'agent'
      AND event_type <> 'replied'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(event_at) OVER (ORDER BY event_at ASC, id) AS prev_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at ASC, id) AS event_at_seq,
        ROUND(PERCENT_RANK() OVER w_actor_ticket_id_by_event_at, 3) AS event_at_percent_rank,
        LEAD(event_at, 3) OVER w_actor_ticket_id_by_event_at_2 AS next3_event_at,
        LEAD(event_at) OVER (ORDER BY event_at ASC, id) AS next_event_at,
        RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id) AS event_at_rank
    FROM recent
    WINDOW w_actor_ticket_id_by_event_at AS (PARTITION BY actor, ticket_id ORDER BY event_at DESC), w_actor_ticket_id_by_event_at_2 AS (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id)
)
SELECT
    event_at_seq,
    COALESCE(event_type, 'ALL') AS event_type,
    GROUPING(event_at_seq) AS is_total_event_at_seq,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
    ROUND(SUM(event_at_seq) / NULLIF(SUM(event_at_percent_rank), 0), 3) AS event_at_seq_to_event_at_percent_rank_ratio
FROM windowed
GROUP BY CUBE(event_at_seq, event_type)
ORDER BY event_at_seq NULLS LAST, event_type NULLS LAST
