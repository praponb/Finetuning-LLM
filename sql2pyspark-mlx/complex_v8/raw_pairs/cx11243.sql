WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority
    FROM cleaned s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM joined_ticket s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at < TIMESTAMP '2025-03-02 10:00:00'
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_ticket_closed_at_month_ticke AS ticket_closed_at_rank,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(LAG(ticket_closed_at) OVER (PARTITION BY DATE_TRUNC('MONTH', ticket_closed_at), ticket_id ORDER BY ticket_closed_at ASC, id))) / 60, 1) AS minutes_since_prev_ticket_closed_at,
        FIRST_VALUE(ticket_closed_at) OVER (PARTITION BY DATE_TRUNC('MONTH', ticket_closed_at), ticket_id ORDER BY ticket_closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_closed_at
    FROM without_matches
    WINDOW w_ticket_closed_at_month_ticke AS (PARTITION BY DATE_TRUNC('MONTH', ticket_closed_at), ticket_id ORDER BY ticket_closed_at ASC)
),
event_type_stats AS (
    -- Median ticket_closed_at_rank per event_type
    SELECT event_type, PERCENTILE_APPROX(ticket_closed_at_rank, 0.5) AS median_ticket_closed_at_rank
    FROM windowed
    GROUP BY event_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ticket_closed_at_rank,
        ROUND(s.ticket_closed_at_rank - g.median_ticket_closed_at_rank, 2) AS ticket_closed_at_rank_minus_median
    FROM windowed s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.ticket_closed_at_rank >= g.median_ticket_closed_at_rank
)
SELECT
    id,
    ticket_id,
    ticket_priority,
    ticket_closed_at_rank,
    minutes_since_prev_ticket_closed_at,
    first_ticket_closed_at,
    LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
    ABS(ticket_closed_at_rank - 4) AS ticket_closed_at_rank_distance
FROM vs_group
ORDER BY ticket_id DESC
