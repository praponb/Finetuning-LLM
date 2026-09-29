WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE actor = 'customer'
      AND ticket_id IS NOT NULL
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type_len,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM valid_events s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN event_type_len >= 9 THEN 'large' WHEN event_type_len >= 5 THEN 'medium' ELSE 'small' END AS event_type_len_tier,
        FLOOR(event_type_len / 2) * 2 AS event_type_len_bucket,
        CASE WHEN event_event_type IN ('reopened', 'resolved') THEN 'group_a' WHEN event_event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_event_type_group
    FROM with_children
    WHERE ticket_id IS NOT NULL
)
SELECT *
FROM (
    SELECT ticket_id, event_type_len_tier, event_type_len
    FROM tagged
) src
PIVOT (
    SUM(event_type_len)
    FOR event_type_len_tier IN ('small', 'medium', 'large')
)
