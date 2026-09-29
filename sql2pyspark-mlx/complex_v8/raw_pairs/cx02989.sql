WITH filtered AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-18'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        DENSE_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC) AS event_at_dense_rank,
        RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC) AS event_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        NTILE(10) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id) AS event_at_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC), 3) AS event_at_percent_rank
    FROM filtered
),
filtered_2 AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at < TIMESTAMP '2025-02-18 11:00:00'
        )
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.first_known_event_at,
        s.event_at_dense_rank,
        s.event_at_rank,
        s.event_at_cume_dist,
        t.agent_id AS ticket_agent_id,
        t.tags AS ticket_tags
    FROM filtered_2 s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE SIZE(t.tags) >= 2
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    ticket_tags,
    ROUND(event_at_dense_rank / NULLIF(event_at_cume_dist, 0), 3) AS event_at_dense_rank_per_event_at_cume_dist
FROM joined
WHERE NOT ARRAY_CONTAINS(ticket_tags, 'outage')
ORDER BY ticket_id NULLS LAST, id
LIMIT 5
