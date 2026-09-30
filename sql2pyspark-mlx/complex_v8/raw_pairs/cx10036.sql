WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year,
        UPPER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE actor = 'customer'
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_year,
        s.actor_clean,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority,
        t.opened_at AS ticket_opened_at
    FROM scoped_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
actor_stats AS (
    -- Avg event_at_year per actor
    SELECT actor, AVG(event_at_year) AS avg_event_at_year
    FROM joined_ticket
    GROUP BY actor
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_at_year,
        ROUND(s.event_at_year - g.avg_event_at_year, 2) AS event_at_year_minus_avg
    FROM joined_ticket s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.event_at_year > g.avg_event_at_year
),
event_type_stats AS (
    -- Avg avg_event_at_year per event_type
    SELECT event_type, AVG(avg_event_at_year) AS avg_avg_event_at_year
    FROM outliers
    GROUP BY event_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_avg_event_at_year,
        ROUND(s.avg_event_at_year - g.avg_avg_event_at_year, 2) AS avg_event_at_year_minus_avg
    FROM outliers s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.avg_event_at_year > g.avg_avg_event_at_year
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_year,
        s.actor_clean,
        s.ticket_closed_at,
        s.ticket_priority,
        s.ticket_opened_at,
        s.avg_event_at_year,
        s.event_at_year_minus_avg,
        s.avg_avg_event_at_year,
        s.avg_event_at_year_minus_avg,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor
    FROM above_average s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(actor, 'ALL') AS actor,
    COALESCE(event_type, 'ALL') AS event_type,
    event_id,
    GROUPING(actor) AS is_total_actor,
    SUM(CASE WHEN ticket_priority = 'P1' THEN avg_avg_event_at_year ELSE 0 END) AS avg_avg_event_at_year_ticket_priority_p1
FROM exploded_detail
GROUP BY GROUPING SETS ((actor, event_type, event_id), (actor))
