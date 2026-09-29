WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('assigned', 'replied') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group,
        LOWER(TRIM(actor)) AS actor_clean,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE event_type = 'reopened'
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor_ref,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM recent s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM detail_rows s
),
labeled AS (
    -- Add calculated columns
    SELECT *, COALESCE(event_event_type, 'UNKNOWN') AS event_event_type_or_unknown
    FROM with_stats
    WHERE event_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_count ASC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_id, event_actor ORDER BY event_count, id, event_id), 3) AS event_count_percent_rank,
        ROW_NUMBER() OVER w_event_id_event_event_at_to_d AS seq_no,
        NTILE(5) OVER (ORDER BY event_count ASC, id, event_id) AS event_count_quintile
    FROM labeled
    WINDOW w_event_id_event_event_at_to_d AS (PARTITION BY event_id, TO_DATE(event_event_at) ORDER BY event_event_at, id, event_id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor_ref,
    event_id,
    event_actor,
    event_event_at,
    event_count,
    first_known_event_at,
    seq_no,
    event_count_quintile
FROM ranked
WHERE actor_ref IS NOT NULL
ORDER BY actor_ref, event_event_at DESC
