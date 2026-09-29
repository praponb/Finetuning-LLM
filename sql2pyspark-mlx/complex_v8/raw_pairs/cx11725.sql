WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label,
        DATE_TRUNC('YEAR', event_at) AS event_at_year
    FROM ticket_events
    WHERE actor IN ('customer', 'agent')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(*) AS event_count_2,
        MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_count_2, 0) AS event_count_2,
        t.first_event_at
    FROM filtered s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(event_count_2, 3) OVER w_event_at_year_by_event_count AS next3_event_count_2,
        ROUND(event_count - AVG(event_count) OVER (PARTITION BY event_count), 2) AS event_count_vs_event_count_avg,
        RANK() OVER (PARTITION BY YEAR(event_at) ORDER BY event_count DESC NULLS FIRST) AS event_count_rank,
        LEAD(event_at_year) OVER (PARTITION BY YEAR(event_at) ORDER BY event_count DESC NULLS FIRST, id) AS next_event_at_year
    FROM profiled
    WINDOW w_event_at_year_by_event_count AS (PARTITION BY YEAR(event_at) ORDER BY event_count DESC NULLS FIRST, id)
),
per_group AS (
    -- Group-level totals
    SELECT
        event_type,
        DATE_TRUNC('MONTH', next_event_at_year) AS next_event_at_year_month,
        event_count_2,
        COUNT(*) FILTER (WHERE actor <> 'system') AS n_actor_system,
        MIN(next_event_at_year) AS first_next_event_at_year,
        MIN(event_count_2) AS min_event_count_2,
        SUM(event_count_vs_event_count_avg) FILTER (WHERE actor = 'agent') AS event_count_vs_event_count_avg_actor_agent
    FROM with_trends
    GROUP BY event_type, DATE_TRUNC('MONTH', next_event_at_year), event_count_2
),
labeled AS (
    -- Add calculated columns
    SELECT
        event_type,
        next_event_at_year_month,
        event_count_2,
        n_actor_system,
        min_event_count_2,
        CONCAT(event_type, '#', CAST(n_actor_system AS STRING)) AS event_type_ref,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown_2
    FROM per_group
),
enriched AS (
    -- Derive helper columns
    SELECT
        event_type,
        next_event_at_year_month,
        event_count_2,
        min_event_count_2,
        event_type_or_unknown,
        event_type_or_unknown_2,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown_3
    FROM labeled
)
-- Final projection
SELECT
    event_type,
    next_event_at_year_month,
    min_event_count_2,
    event_type_or_unknown,
    event_type_or_unknown_2,
    event_type_or_unknown_3,
    HOUR(next_event_at_year_month) AS next_event_at_year_month_hour
FROM enriched
WHERE next_event_at_year_month < TIMESTAMP '2025-02-01 09:00:00'
ORDER BY event_type_or_unknown_3 DESC
