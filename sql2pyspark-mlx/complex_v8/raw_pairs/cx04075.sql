WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('escalated', 'created') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        LOWER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE event_type NOT IN ('resolved', 'replied')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY actor) AS rows_in_actor,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER w_actor, 2) AS days_since_event_at_vs_actor_avg
    FROM valid_events
    WINDOW w_actor AS (PARTITION BY actor)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_group,
        days_since_event_at,
        actor_clean,
        rows_in_actor,
        days_since_event_at_vs_actor_avg,
        SUBSTRING(TRIM(actor_clean), 1, 3) AS actor_clean_prefix
    FROM with_windows
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_type_group,
        days_since_event_at,
        actor_clean,
        rows_in_actor,
        days_since_event_at_vs_actor_avg,
        actor_clean_prefix,
        COALESCE(event_type_group, 'UNKNOWN') AS event_type_group_or_unknown
    FROM enriched
    WHERE days_since_event_at BETWEEN 175 AND 455
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, amount
FROM (
    SELECT id, rows_in_actor, days_since_event_at
    FROM enriched_2
) src
UNPIVOT (
    amount FOR metric_name IN (rows_in_actor AS m_rows_in_actor, days_since_event_at AS days_since_event_at_v)
) unp
