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
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IN ('agent', 'customer')
              AND t.event_at < TIMESTAMP '2025-03-08 09:00:00'
        )
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        actor,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at ASC, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at ASC), 3) AS event_at_cume_dist,
        LAG(event_at, 3) OVER (PARTITION BY event_type ORDER BY event_at ASC, id) AS prev3_event_at
    FROM qualified
),
unique_rows AS (
    -- Keep the latest row per actor
    SELECT
        id,
        event_at,
        actor,
        minutes_since_prev_event_at,
        event_at_cume_dist,
        prev3_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY prev3_event_at DESC, id DESC) AS row_num
        FROM with_trends
    ) ranked
    WHERE row_num = 1
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        actor,
        minutes_since_prev_event_at,
        DATE_TRUNC('QUARTER', prev3_event_at) AS prev3_event_at_quarter,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM unique_rows
    WHERE event_at >= TIMESTAMP '2025-01-25 10:00:00'
),
summary AS (
    -- Group-level totals
    SELECT
        actor,
        DATE_TRUNC('MONTH', prev3_event_at_quarter) AS prev3_event_at_quarter_month,
        YEAR(event_at) AS event_at_year,
        ROUND(SUM(event_at_dayofweek) / NULLIF(SUM(minutes_since_prev_event_at), 0), 3) AS event_at_dayofweek_to_minutes_since_prev_event_a,
        COUNT(*) AS cnt
    FROM annotated
    GROUP BY actor, DATE_TRUNC('MONTH', prev3_event_at_quarter), YEAR(event_at)
    HAVING COUNT(*) >= 3
),
scored AS (
    -- Label and score rows
    SELECT *, COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM summary
)
SELECT
    actor,
    prev3_event_at_quarter_month,
    event_at_dayofweek_to_minutes_since_prev_event_a,
    cnt,
    actor_or_unknown
FROM scored
ORDER BY prev3_event_at_quarter_month
