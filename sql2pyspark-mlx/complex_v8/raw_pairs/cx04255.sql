WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM ticket_events
    WHERE (actor = 'agent' OR MONTH(event_at) IN (2, 3))
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN event_type IN ('reopened', 'resolved') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix,
        CONCAT(event_at_label, '#', CAST(ticket_id AS STRING)) AS event_at_label_ref
    FROM scoped_events
    WHERE event_at_label IS NOT NULL
)
SELECT
    event_type_group,
    event_type,
    GROUPING(event_type_group) AS is_total_event_type_group,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
    COUNT(*) FILTER (WHERE event_type NOT IN ('resolved', 'escalated')) AS n_event_type_not_in_resolved_e
FROM tagged
GROUP BY ROLLUP(event_type_group, event_type)
