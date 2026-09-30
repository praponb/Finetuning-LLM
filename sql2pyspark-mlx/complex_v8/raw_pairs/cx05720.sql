WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        CONCAT_WS('|', actor, event_type) AS actor_event_type_label,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_len,
        s.actor_event_type_label,
        t.closed_at AS ticket_closed_at
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id AND t.channel <> 'web'
),
joined_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.opened_at AS ticket_opened_at,
        t.csat AS ticket_csat,
        t.channel AS ticket_channel
    FROM with_details s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', ticket_opened_at) AS ticket_opened_at_month,
        SUM(actor_len) AS total_actor_len,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(actor_len), 2) AS stddev_actor_len,
        ROUND(AVG(actor_len), 2) AS avg_actor_len,
        MAX(actor_len) AS max_actor_len
    FROM joined_ticket
    GROUP BY 1
    HAVING SUM(actor_len) > 4
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM summary
    WHERE total_actor_len < (SELECT MAX(total_actor_len) FROM summary)
)
-- Final projection
SELECT
    ticket_opened_at_month,
    stddev_actor_len,
    avg_actor_len,
    max_actor_len,
    stddev_actor_len + total_actor_len AS stddev_actor_len_plus_total_actor_len
FROM with_activity
ORDER BY avg_actor_len
