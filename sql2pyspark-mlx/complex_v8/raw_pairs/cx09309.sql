WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        CASE WHEN event_type IN ('escalated', 'assigned') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE event_type IN ('created', 'escalated')
),
qualified AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM cleaned_events s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.priority IN ('P4', 'P3', 'P1')
              AND MONTH(t.closed_at) IN (1, 3)
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER w_event_type_group_by_event_at AS event_at_decile,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        NTILE(4) OVER (PARTITION BY event_type_group ORDER BY event_at DESC, id) AS event_at_quartile,
        LEAD(event_at, 3) OVER w_all_by_event_at AS next3_event_at
    FROM qualified
    WINDOW w_event_type_group_by_event_at AS (PARTITION BY event_type_group ORDER BY event_at DESC, id), w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        COUNT(*) OVER (PARTITION BY event_at_quartile) AS rows_in_event_at_quartile
    FROM metrics
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.channel AS ticket_channel,
        t.agent_id AS ticket_agent_id,
        t.tags AS ticket_tags
    FROM with_windows s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.channel IS NOT NULL
)
SELECT *
FROM (
    SELECT ticket_agent_id, event_type_group, rows_in_event_at_quartile
    FROM enriched_ticket
) src
PIVOT (
    SUM(rows_in_event_at_quartile)
    FOR event_type_group IN ('group_a', 'group_b', 'other')
)
