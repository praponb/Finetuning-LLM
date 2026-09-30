WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at_2
    FROM ticket_events
    WHERE (actor IS NOT NULL OR ticket_id >= 58)
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor NOT IN ('customer', 'system')
        )
),
unique_rows AS (
    -- Keep the latest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        event_at_label,
        days_since_event_at_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY days_since_event_at_2 DESC, id DESC) AS row_num
        FROM screened
    ) ranked
    WHERE row_num = 1
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MIN(event_at) AS first_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.first_event_at_2
    FROM unique_rows s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.actor = 'system') FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count,
        (SELECT COUNT(CASE WHEN t.event_type = 'assigned' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at_3
    FROM with_stats s
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(event_actor_count - AVG(event_actor_count) OVER (PARTITION BY MONTH(first_event_at_2)), 2) AS event_actor_count_vs_first_event_at_2_month_avg,
        FIRST_VALUE(days_since_event_at_2) IGNORE NULLS OVER (PARTITION BY actor ORDER BY first_event_at_3 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_event_at_2
    FROM summarized_rows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY days_since_event_at_2, id), 3) AS days_since_event_at_2_percent_rank,
        MIN(days_since_event_at_2) OVER (PARTITION BY TO_DATE(first_event_at)) AS first_event_at_to_date_min_days_since_event_at_2,
        LAG(days_since_event_at_2) OVER w_all_by_days_since_event_at_2 AS prev_days_since_event_at_2,
        LAST_VALUE(event_at) OVER (PARTITION BY event_event_type_count ORDER BY event_actor_count_vs_first_event_at_2_month_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM with_windows
    WINDOW w_all_by_days_since_event_at_2 AS (ORDER BY days_since_event_at_2, id)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    days_since_event_at,
    event_at_label,
    days_since_event_at_2,
    first_event_at,
    event_actor_count,
    event_event_type_count,
    first_event_at_3,
    event_actor_count_vs_first_event_at_2_month_avg,
    first_known_days_since_event_at_2,
    first_event_at_to_date_min_days_since_event_at_2,
    prev_days_since_event_at_2,
    last_event_at,
    DATEDIFF(DATE '2025-12-31', first_event_at_2) AS days_since_first_event_at_2,
    COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
FROM metrics
WHERE days_since_event_at_2_percent_rank > 0.1
ORDER BY event_actor_count DESC
