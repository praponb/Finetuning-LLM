WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 7 DAYS AS event_at_plus_7d,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        actor,
        event_at_plus_7d,
        event_at_plus_30m,
        TO_DATE(event_at_plus_7d) AS event_at_plus_7d_date,
        ROUND((UNIX_TIMESTAMP(event_at_plus_7d) - UNIX_TIMESTAMP(event_at_plus_30m)) / 3600, 1) AS hours_event_at_plus_30m_to_event_at_plus_7d
    FROM cleaned
),
enriched_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_at_plus_30m,
        s.event_at_plus_7d_date,
        s.hours_event_at_plus_30m_to_event_at_plus_7d,
        t.tags AS ticket_tags,
        t.requester_id AS ticket_requester_id
    FROM derived s
    JOIN tickets t ON t.id = s.ticket_id AND HOUR(t.closed_at) BETWEEN 10 AND 13
),
scored AS (
    -- Label and score rows
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM enriched_ticket
    WHERE SIZE(ticket_tags) >= 2
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_at_plus_30m) AS event_at_plus_30m_month,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        SUM(CASE WHEN actor IN ('customer', 'system') THEN ticket_requester_id ELSE 0 END) AS ticket_requester_id_actor_in_customer_system,
        COUNT(DISTINCT actor) AS distinct_actors
    FROM scored
    GROUP BY DATE_TRUNC('MONTH', event_at_plus_30m)
    HAVING MAX(hours_event_at_plus_30m_to_event_at_plus_7d) > -1200.0
),
annotated AS (
    -- Add calculated columns
    SELECT
        event_at_plus_30m_month,
        actor_list,
        ticket_requester_id_actor_in_customer_system,
        distinct_actors,
        DAYOFWEEK(event_at_plus_30m_month) AS event_at_plus_30m_month_dayofweek
    FROM stats
    WHERE TO_DATE(event_at_plus_30m_month) < DATE '2025-02-01'
)
SELECT
    event_at_plus_30m_month,
    actor_list,
    ticket_requester_id_actor_in_customer_system,
    distinct_actors,
    FLOOR(distinct_actors / 2) * 2 AS distinct_actors_bucket,
    FLOOR(distinct_actors / 2) * 2 AS distinct_actors_bucket_2
FROM annotated
ORDER BY distinct_actors_bucket_2 DESC, event_at_plus_30m_month
LIMIT 20
