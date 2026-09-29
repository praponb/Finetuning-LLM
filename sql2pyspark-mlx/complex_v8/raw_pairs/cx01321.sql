WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
),
totals AS (
    -- Summarise per group
    SELECT
        ticket_id,
        event_type,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_ym,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        SUM(days_since_event_at) AS total_days_since_event_at,
        MAX(days_since_event_at) AS max_days_since_event_at
    FROM cleaned
    WHERE event_at < TIMESTAMP '2025-03-02 09:00:00'
    GROUP BY ticket_id, event_type, DATE_FORMAT(event_at, 'yyyy-MM')
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.ticket_id,
        s.event_type,
        s.event_at_ym,
        s.total_days_since_event_at,
        t.id AS event_id,
        t.event_at,
        t.event_type AS event_event_type
    FROM totals s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY event_id, event_event_type)) AS distinct_ticket_id_in_event_id_event_event_type,
        MAX(total_days_since_event_at) OVER (PARTITION BY event_type ORDER BY event_at, ticket_id, event_type, event_at_ym, event_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS total_days_since_event_at_max_3_prior,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY event_id, event_event_type)) AS distinct_event_type_in_event_id_event_event_type,
        ROW_NUMBER() OVER w_event_type_by_event_at AS event_at_seq,
        total_days_since_event_at - LAG(total_days_since_event_at) OVER (PARTITION BY event_id, ticket_id ORDER BY total_days_since_event_at ASC, ticket_id, event_type, event_at_ym, event_id) AS total_days_since_event_at_change,
        ROUND(total_days_since_event_at - AVG(total_days_since_event_at) OVER (PARTITION BY event_id, event_event_type), 2) AS total_days_since_event_at_vs_event_id_event_even
    FROM detail_rows_events
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at, ticket_id, event_type, event_at_ym, event_id)
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.ticket_id,
        s.event_type,
        s.event_at_ym,
        s.total_days_since_event_at,
        s.event_id,
        s.event_event_type,
        s.distinct_ticket_id_in_event_id_event_event_type,
        s.total_days_since_event_at_max_3_prior,
        s.distinct_event_type_in_event_id_event_event_type,
        s.event_at_seq,
        s.total_days_since_event_at_vs_event_id_event_even,
        t.ticket_id AS event_ticket_id,
        t.actor AS event_actor,
        ti.tags AS ticket_tags,
        ti.channel AS ticket_channel
    FROM with_windows s
    JOIN ticket_events t ON t.id = s.event_id
    LEFT JOIN tickets ti ON ti.id = s.ticket_id
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IS NOT NULL
              AND TO_DATE(t.event_at) < DATE '2025-02-12'
        )
      AND s.event_id <= 25
)
-- Final projection
SELECT
    ticket_id,
    event_type,
    event_at_ym,
    total_days_since_event_at,
    event_event_type,
    distinct_ticket_id_in_event_id_event_event_type,
    distinct_event_type_in_event_id_event_event_type,
    total_days_since_event_at_vs_event_id_event_even,
    event_ticket_id,
    event_actor,
    ticket_tags,
    ticket_channel,
    SUBSTRING(TRIM(event_actor), 1, 3) AS event_actor_prefix
FROM with_activity
ORDER BY total_days_since_event_at DESC, ticket_channel NULLS LAST
