WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label_2
    FROM ticket_events
    WHERE event_type IS NOT NULL
      AND actor IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label_2
    FROM valid_events
    WHERE event_type <> 'resolved'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_actor_label_2,
        NTILE(5) OVER (PARTITION BY event_type ORDER BY event_at, id) AS event_at_quintile,
        LEAD(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at, id) AS next_event_at
    FROM enriched
),
derived AS (
    -- Label and score rows
    SELECT *, COALESCE(event_at_quintile, 0) AS event_at_quintile_filled
    FROM with_windows
)
SELECT
    actor,
    event_at_quintile,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows,
    SUM(CASE WHEN event_type IN ('resolved', 'reopened', 'created') THEN event_at_quintile_filled ELSE 0 END) AS event_at_quintile_filled_event_type_in_resolved_
FROM derived
GROUP BY GROUPING SETS ((actor, event_at_quintile), (actor), (event_at_quintile))
