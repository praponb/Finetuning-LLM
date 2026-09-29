WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        TO_DATE(event_at) AS event_at_date,
        QUARTER(event_at) AS event_at_quarter
    FROM ticket_events
    WHERE event_type IS NOT NULL
      AND actor = 'system'
),
latest AS (
    -- Keep the earliest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_dayofweek,
        event_at_date,
        event_at_quarter
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at_dayofweek, id) AS recency_rank
        FROM scoped_events
    ) ranked
    WHERE recency_rank = 1
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM latest s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type <> 'created'
              AND t.event_at < TIMESTAMP '2025-03-08 09:00:00'
        )
)
SELECT
    event_at_quarter,
    COALESCE(actor, 'ALL') AS actor,
    PERCENTILE_APPROX(event_at_quarter, 0.5) AS median_event_at_quarter,
    ROUND(AVG(event_at_quarter), 2) AS avg_event_at_quarter,
    COUNT(*) FILTER (WHERE event_type <> 'reopened') AS n_event_type_reopened
FROM qualified
GROUP BY CUBE(event_at_quarter, actor)
ORDER BY event_at_quarter NULLS LAST, actor NULLS LAST
