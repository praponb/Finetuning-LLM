WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        LENGTH(TRIM(actor)) AS actor_len,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d
    FROM ticket_events
    WHERE ticket_id <= 13
),
enriched AS (
    -- Attach ticket attributes
    SELECT s.*, t.requester_id AS ticket_requester_id, t.agent_id AS ticket_agent_id
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id AND t.closed_at >= TIMESTAMP '2025-01-16 00:00:00'
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_at_plus_1d,
        s.ticket_requester_id,
        s.ticket_agent_id,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM enriched s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM line_level_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
event_actor_stats AS (
    -- Median ticket_requester_id per event_actor
    SELECT event_actor, PERCENTILE_APPROX(ticket_requester_id, 0.5) AS median_ticket_requester_id
    FROM profiled
    GROUP BY event_actor
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ticket_requester_id,
        ROUND(s.ticket_requester_id - g.median_ticket_requester_id, 2) AS ticket_requester_id_minus_median
    FROM profiled s
    JOIN event_actor_stats g ON g.event_actor = s.event_actor
    WHERE s.ticket_requester_id >= g.median_ticket_requester_id
),
summary AS (
    -- Summarise per group
    SELECT
        event_id,
        event_actor,
        event_count,
        COUNT(*) AS row_count,
        SUM(ticket_requester_id) AS total_ticket_requester_id,
        SUM(ticket_requester_id) FILTER (WHERE event_actor <> 'customer') AS ticket_requester_id_event_actor_customer,
        SUM(median_ticket_requester_id) AS total_median_ticket_requester_id,
        SUM(event_count) FILTER (WHERE event_type = 'escalated') AS event_count_event_type_escalated
    FROM vs_group
    GROUP BY event_id, event_actor, event_count
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        event_id,
        event_actor,
        event_count,
        total_ticket_requester_id,
        ticket_requester_id_event_actor_customer,
        total_median_ticket_requester_id,
        NTH_VALUE(total_ticket_requester_id, 2) OVER (ORDER BY row_count ASC, event_id, event_actor, event_count ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_ticket_requester_id,
        FIRST_VALUE(row_count) IGNORE NULLS OVER (PARTITION BY event_actor, row_count ORDER BY ticket_requester_id_event_actor_customer DESC, event_id, event_actor, event_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_count
    FROM summary
)
SELECT
    event_id,
    event_actor,
    total_ticket_requester_id,
    ticket_requester_id_event_actor_customer,
    total_median_ticket_requester_id,
    second_total_ticket_requester_id,
    event_count + first_known_row_count AS event_count_plus_first_known_row_count
FROM ranked
ORDER BY event_id, total_median_ticket_requester_id DESC
