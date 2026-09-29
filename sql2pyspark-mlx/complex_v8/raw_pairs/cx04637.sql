WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM scoped s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.event_at >= TIMESTAMP '2025-01-31 13:00:00'
        )
      AND s.actor NOT IN ('customer', 'system')
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.event_type <> 'replied') FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at
    FROM without_matches s
),
rollup_base AS (
    -- Summarise per group
    SELECT
        event_type,
        YEAR(event_at) AS event_at_year,
        event_count,
        MAX(event_event_type_count) AS max_event_event_type_count,
        MAX(first_event_at) AS last_first_event_at,
        ROUND(STDDEV(event_event_type_count), 2) AS stddev_event_event_type_count,
        MIN(first_event_at) AS first_first_event_at,
        COUNT(*) AS row_count
    FROM with_counts
    GROUP BY 1, 2, 3
),
derived AS (
    -- Add calculated columns
    SELECT
        event_type,
        event_at_year,
        event_count,
        max_event_event_type_count,
        last_first_event_at,
        stddev_event_event_type_count,
        row_count,
        CASE WHEN event_type IN ('escalated', 'created') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM rollup_base
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(event_type_group), ' ', '_')) AS event_type_group_slug,
        ABS(row_count - 5) AS row_count_distance
    FROM derived
    WHERE event_count < 5
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM annotated
    WHERE event_at_year > (SELECT MIN(event_at_year) FROM annotated)
)
SELECT
    event_type,
    event_at_year,
    event_count,
    max_event_event_type_count,
    last_first_event_at,
    stddev_event_event_type_count,
    row_count,
    event_type_group,
    event_type_group_slug,
    row_count_distance
FROM with_activity
ORDER BY event_type_group_slug DESC
