WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        CASE WHEN event_type IN ('reopened', 'escalated') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE actor = 'system'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(*) AS event_count,
        COUNT(CASE WHEN event_type IN ('escalated', 'assigned') THEN 1 END) AS event_event_type_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count
    FROM recent_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_slug,
        s.event_type_group,
        s.last_event_at,
        s.event_event_type_count,
        t.agent_id AS ticket_agent_id
    FROM summarized_rows s
    JOIN tickets t ON t.id = s.ticket_id
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(last_event_at, 'yyyy-MM') AS last_event_at_ym,
        ROUND(AVG(event_event_type_count), 2) AS avg_event_event_type_count,
        COUNT(DISTINCT actor) AS distinct_actors,
        MAX(event_at) AS last_event_at_2,
        MIN(last_event_at) AS first_last_event_at
    FROM combined_details
    GROUP BY DATE_FORMAT(last_event_at, 'yyyy-MM')
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE avg_event_event_type_count < (SELECT MAX(avg_event_event_type_count) FROM totals)
)
SELECT
    last_event_at_ym,
    avg_event_event_type_count,
    distinct_actors,
    last_event_at_2,
    LENGTH(TRIM(last_event_at_ym)) AS last_event_at_ym_len
FROM filtered
WHERE distinct_actors < 6
ORDER BY avg_event_event_type_count NULLS LAST, last_event_at_ym
