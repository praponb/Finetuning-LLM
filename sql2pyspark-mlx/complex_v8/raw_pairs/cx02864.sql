WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE ticket_id >= 13
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.actor_slug,
        s.event_at_date,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags,
        su.tier AS agent_tier
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id AND t.priority = 'P4'
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
per_label AS (
    -- One row per ticket_tag
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.actor_slug,
        s.event_at_date,
        s.ticket_requester_id,
        s.agent_tier,
        t.ticket_tag
    FROM combined_details s
    LATERAL VIEW OUTER EXPLODE(s.ticket_tags) t AS ticket_tag
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM per_label
    WHERE ticket_requester_id > (SELECT MIN(ticket_requester_id) FROM per_label)
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM without_matches s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IN ('reopened', 'resolved', 'created')
        )
      AND s.event_at_date IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, t.tags AS ticket_tags, t.requester_id AS ticket_requester_id_2
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    actor_slug,
    event_at_date,
    ticket_requester_id,
    ticket_tags,
    ticket_requester_id_2,
    ARRAY_CONTAINS(ticket_tags, 'refund') AS has_refund,
    DATE_ADD(event_at_date, 7) AS event_at_date_plus_7d
FROM joined
ORDER BY event_at_date, ticket_requester_id DESC
