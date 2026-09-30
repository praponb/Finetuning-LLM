WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        COALESCE(csat, 0) AS csat_filled,
        MONTH(opened_at) AS opened_at_month
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 7 AND 12
      AND requester_id <= 17
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        opened_at_month,
        ROUND(CUME_DIST() OVER (ORDER BY csat_filled DESC NULLS FIRST, id), 3) AS csat_filled_cume_dist,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY channel, csat_filled), 0), 4) AS csat_share_of_channel_csat_filled,
        LAG(opened_at_month) OVER (ORDER BY csat_filled DESC NULLS FIRST, id) AS prev_opened_at_month,
        ROUND(AVG(csat) OVER (PARTITION BY channel, csat_filled), 2) AS channel_csat_filled_avg_csat
    FROM base_tickets
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND HOUR(t.event_at) BETWEEN 8 AND 11
        )
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
        )
),
screened_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM without_matches
    WHERE opened_at_month > (SELECT AVG(opened_at_month) FROM without_matches)
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.csat,
        s.prev_opened_at_month,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM screened_2 s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.event_type = 'reopened' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_event_type_count
    FROM line_level s
)
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    event_event_type_count,
    ROUND(AVG(prev_opened_at_month), 2) AS avg_prev_opened_at_month
FROM summarized_rows
GROUP BY GROUPING SETS ((event_type, event_event_type_count), (event_type), ())
ORDER BY event_type NULLS LAST, event_event_type_count NULLS LAST
