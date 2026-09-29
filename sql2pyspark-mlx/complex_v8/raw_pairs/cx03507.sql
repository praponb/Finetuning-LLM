WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('resolved', 'reopened') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
    WHERE event_type NOT IN ('resolved', 'replied')
      AND ticket_id >= 2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY event_type, actor ORDER BY event_at, id) AS event_at_decile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LEAD(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at, id) AS next_event_at,
        RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at) AS event_at_rank
    FROM scoped
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor NOT IN ('system', 'agent')
              AND TO_DATE(t.event_at) < DATE '2025-03-08'
        )
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, t.requester_id AS ticket_requester_id
    FROM without_matches s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.requester_id > 7
)
SELECT
    event_at_rank,
    actor,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows
FROM with_parent
GROUP BY CUBE(event_at_rank, actor)
ORDER BY event_at_rank NULLS LAST, actor NULLS LAST
