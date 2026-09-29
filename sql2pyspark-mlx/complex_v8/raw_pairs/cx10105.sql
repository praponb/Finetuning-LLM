WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE actor NOT IN ('system', 'agent')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count, COUNT(*) AS event_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_count_2, 0) AS event_count_2
    FROM recent s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
latest_per_key AS (
    -- Keep the latest row per event_type, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_week,
        event_count,
        event_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY event_count_2 DESC, id DESC) AS row_num
        FROM with_stats
    ) ranked
    WHERE row_num = 1
),
rollup_base AS (
    -- Summarise per group
    SELECT
        event_type,
        event_count,
        DATE_TRUNC('MONTH', event_at_week) AS event_at_week_month,
        SUM(CASE WHEN actor IN ('system', 'agent') THEN 1 ELSE 0 END) AS n_actor_in_system_agent,
        SUM(CASE WHEN actor IS NOT NULL THEN event_count_2 ELSE 0 END) AS event_count_2_actor_is_not_null,
        COUNT(*) AS cnt,
        MIN(event_at) AS first_event_at,
        SUM(event_count) AS total_event_count
    FROM latest_per_key
    GROUP BY event_type, event_count, DATE_TRUNC('MONTH', event_at_week)
    HAVING COUNT(*) >= 3
)
SELECT
    event_type,
    event_count,
    cnt,
    first_event_at,
    total_event_count,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
    SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
FROM rollup_base
WHERE event_count BETWEEN 2 AND 6
ORDER BY event_type_or_unknown DESC NULLS FIRST, total_event_count
