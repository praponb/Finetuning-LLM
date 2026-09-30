WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label_2
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT *, YEAR(event_at) AS event_at_year
    FROM filtered_events
),
actor_stats AS (
    -- Avg event_at_year per actor
    SELECT actor, AVG(event_at_year) AS avg_event_at_year
    FROM annotated
    GROUP BY actor
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_at_year,
        ROUND(s.event_at_year - g.avg_event_at_year, 2) AS event_at_year_minus_avg
    FROM annotated s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.event_at_year > g.avg_event_at_year
),
deduped AS (
    -- Keep the latest row per actor, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_label,
        event_at_label_2,
        event_at_year,
        avg_event_at_year,
        event_at_year_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, event_type ORDER BY event_at_year DESC, id DESC) AS rn
        FROM above_average
    ) ranked
    WHERE rn = 1
)
SELECT
    actor,
    event_at_year,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(avg_event_at_year), 2) AS stddev_avg_event_at_year
FROM deduped
GROUP BY GROUPING SETS ((actor, event_at_year), (actor), ())
ORDER BY actor NULLS LAST, event_at_year NULLS LAST
