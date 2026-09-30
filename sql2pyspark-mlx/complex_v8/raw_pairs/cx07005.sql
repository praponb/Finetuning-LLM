WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('DAY', event_at) AS event_at_day,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE actor = 'customer'
      AND ticket_id IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at_day ASC, id) AS rn,
        DENSE_RANK() OVER (ORDER BY event_at_day) AS event_at_day_dense_rank
    FROM filtered
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_day,
        s.event_at_date,
        s.rn,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM metrics s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type <> 'reopened'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_at_day,
        event_id,
        event_actor,
        event_event_type,
        ROUND(rn / NULLIF(SUM(rn) OVER (PARTITION BY event_id, event_actor), 0), 4) AS rn_share_of_event_id_event_actor,
        DENSE_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_event_at DESC) AS event_event_at_dense_rank
    FROM with_children_events
),
with_parent_event AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.event_type,
        s.actor,
        s.event_at_day,
        s.event_id,
        s.event_event_type,
        s.rn_share_of_event_id_event_actor,
        s.event_event_at_dense_rank,
        t.event_type AS event_event_type_2,
        t.actor AS event_actor_2,
        t.event_at AS event_event_at
    FROM sequenced s
    JOIN ticket_events t ON t.id = s.event_id
    WHERE t.actor IN ('agent', 'customer')
),
leaders AS (
    -- First row per group
    SELECT *
    FROM with_parent_event
    WHERE event_event_at_dense_rank = 1
),
qualified AS (
    -- Keep rows whose event matches
    SELECT s.*
    FROM leaders s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.id = s.event_id
              AND t.actor IS NOT NULL
              AND t.event_type = 'reopened'
        )
)
SELECT
    id,
    event_type,
    actor,
    event_id,
    event_event_at_dense_rank,
    event_actor_2,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
    CONCAT(actor, '#', CAST(event_id AS STRING)) AS actor_ref
FROM qualified
ORDER BY event_id DESC
