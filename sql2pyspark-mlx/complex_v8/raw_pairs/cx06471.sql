WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        LENGTH(TRIM(actor)) AS actor_len_2,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 08:00:00'
      AND event_type <> 'escalated'
),
grouped AS (
    -- Group-level totals
    SELECT
        actor,
        COUNT(*) FILTER (WHERE event_type IS NOT NULL) AS n_event_type_is_not_null,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(DISTINCT event_type_or_unknown) AS distinct_event_type_or_unknowns,
        COUNT(DISTINCT id) AS distinct_ids
    FROM base_events
    GROUP BY actor
    HAVING COUNT(*) >= 3
),
flattened AS (
    -- One row per actor_list_item
    SELECT
        s.actor,
        s.n_event_type_is_not_null,
        s.cnt,
        s.distinct_event_type_or_unknowns,
        s.distinct_ids,
        t.actor_list_item
    FROM grouped s
    LATERAL VIEW EXPLODE(s.actor_list) t AS actor_list_item
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug_2
    FROM flattened
    WHERE actor = 'system'
),
labeled AS (
    -- Label and score rows
    SELECT
        actor,
        n_event_type_is_not_null,
        cnt,
        distinct_event_type_or_unknowns,
        actor_list_item,
        actor_slug,
        actor_slug_2,
        FLOOR(distinct_ids / 2) * 2 AS distinct_ids_bucket
    FROM enriched
    WHERE cnt BETWEEN 5 AND 9
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE n_event_type_is_not_null > (SELECT AVG(n_event_type_is_not_null) FROM labeled)
)
-- Final projection
SELECT
    actor,
    n_event_type_is_not_null,
    distinct_event_type_or_unknowns,
    actor_list_item,
    actor_slug,
    actor_slug_2,
    distinct_ids_bucket
FROM qualified
WHERE distinct_event_type_or_unknowns >= 5
ORDER BY actor, actor_slug_2
