WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
        CASE WHEN event_type IN ('assigned', 'escalated') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE actor IS NOT NULL
      AND event_at < TIMESTAMP '2025-02-24 13:00:00'
),
summary AS (
    -- Group-level totals
    SELECT
        event_type,
        actor,
        SUM(CASE WHEN event_type = 'escalated' THEN 1 ELSE 0 END) AS n_event_type_escalated,
        ROUND(100 * COUNT(CASE WHEN event_type <> 'reopened' THEN 1 END) / COUNT(*), 1) AS pct_event_type_reopened,
        COUNT(*) FILTER (WHERE event_type IS NOT NULL) AS n_event_type_is_not_null
    FROM cleaned_events
    GROUP BY event_type, actor
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        ABS(n_event_type_escalated - 6) AS n_event_type_escalated_distance
    FROM summary
),
tagged_2 AS (
    -- Add calculated columns
    SELECT
        event_type,
        actor,
        n_event_type_escalated_distance,
        ROUND(pct_event_type_reopened * 1.1, 2) AS pct_event_type_reopened_adj
    FROM tagged
    WHERE pct_event_type_reopened < 90.0
)
SELECT
    event_type,
    actor,
    n_event_type_escalated_distance,
    pct_event_type_reopened_adj,
    CONCAT_WS('-', actor, event_type) AS actor_event_type_label
FROM tagged_2
ORDER BY pct_event_type_reopened_adj NULLS LAST, n_event_type_escalated_distance DESC, event_type, actor
LIMIT 5
