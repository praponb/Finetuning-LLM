WITH valid_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE ticket_id >= 33
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id AND t.requester_id > 4
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.ticket_tags,
        s.ticket_closed_at,
        t.tags AS ticket_tags_2,
        t.requester_id AS ticket_requester_id
    FROM with_parent s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.ticket_closed_at,
        s.ticket_tags_2,
        s.ticket_requester_id,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type,
        t.actor AS event_actor
    FROM enriched s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_event_at) AS event_event_at_month,
        DATE_FORMAT(ticket_closed_at, 'yyyy-MM') AS ticket_closed_at_ym,
        SUM(ticket_requester_id) FILTER (WHERE event_actor = 'customer') AS ticket_requester_id_event_actor_customer,
        ROUND(100 * COUNT(CASE WHEN event_type = 'reopened' THEN 1 END) / COUNT(*), 1) AS pct_event_type_reopened
    FROM exploded_detail
    WHERE ticket_id >= 33
    GROUP BY 1, 2
)
-- Final projection
SELECT
    event_event_at_month,
    ticket_closed_at_ym,
    ticket_requester_id_event_actor_customer,
    LOWER(REGEXP_REPLACE(TRIM(ticket_closed_at_ym), ' ', '_')) AS ticket_closed_at_ym_slug,
    SUBSTRING(TRIM(ticket_closed_at_ym), 1, 2) AS ticket_closed_at_ym_prefix
FROM per_group
ORDER BY ticket_closed_at_ym_slug DESC
