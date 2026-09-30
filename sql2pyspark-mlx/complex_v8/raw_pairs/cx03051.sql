WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('replied', 'created') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE ticket_id >= 8
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        ticket_id,
        event_type,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(event_at) AS first_event_at,
        COUNT(*) FILTER (WHERE event_type IN ('reopened', 'created', 'resolved')) AS n_event_type_in_reopened_creat,
        COUNT(*) AS row_count,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
    FROM valid
    GROUP BY actor, ticket_id, event_type
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        ABS(distinct_ticket_ids - 4) AS distinct_ticket_ids_distance,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM grouped
),
deduped AS (
    -- Keep the earliest row per event_type_or_unknown
    SELECT
        actor,
        ticket_id,
        event_type,
        distinct_ids,
        first_event_at,
        n_event_type_in_reopened_creat,
        row_count,
        distinct_ticket_ids,
        event_type_list,
        event_type_or_unknown,
        distinct_ticket_ids_distance,
        actor_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type_or_unknown ORDER BY first_event_at, actor, ticket_id, event_type) AS recency_rank
        FROM labeled
    ) ranked
    WHERE recency_rank = 1
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM deduped s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.event_type IN ('escalated', 'resolved', 'created')
              AND t.event_at >= TIMESTAMP '2025-01-25 07:00:00'
        )
      AND s.actor = 'agent'
)
SELECT
    actor,
    event_type,
    distinct_ids,
    first_event_at,
    n_event_type_in_reopened_creat,
    row_count,
    distinct_ticket_ids,
    event_type_or_unknown,
    COALESCE(row_count, 0) AS row_count_filled
FROM with_activity
ORDER BY n_event_type_in_reopened_creat DESC NULLS FIRST
