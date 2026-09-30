WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 10 AND 16
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(event_at_year - 2025) AS event_at_year_distance,
        ABS(event_at_year - 2025) AS event_at_year_distance_2,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label
    FROM valid
    WHERE event_at_year < 2026
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM derived s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
grouped AS (
    -- Group-level totals
    SELECT
        YEAR(event_at) AS event_at_year_2,
        event_type,
        COUNT(*) AS row_count,
        SUM(event_at_year) AS total_event_at_year,
        SUM(event_at_year) FILTER (WHERE event_type IN ('assigned', 'escalated', 'replied')) AS event_at_year_event_type_in_assigned_escal,
        ROUND(SUM(event_at_year_distance_2) / NULLIF(SUM(event_at_year_distance), 0), 3) AS event_at_year_distance_2_to_event_at_year_distan,
        ROUND(AVG(event_at_year), 2) AS avg_event_at_year,
        MAX(last_event_at) AS last_last_event_at
    FROM summarized_rows
    WHERE actor = 'customer'
    GROUP BY YEAR(event_at), event_type
    HAVING COUNT(*) >= 2
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM grouped
    WHERE event_at_year_event_type_in_assigned_escal > (SELECT MIN(event_at_year_event_type_in_assigned_escal) FROM grouped)
)
-- Final projection
SELECT
    event_at_year_2,
    row_count,
    total_event_at_year,
    event_at_year_event_type_in_assigned_escal,
    event_at_year_distance_2_to_event_at_year_distan,
    last_last_event_at,
    FLOOR(total_event_at_year / 5000) * 5000 AS total_event_at_year_bucket,
    LENGTH(TRIM(event_type)) AS event_type_len
FROM without_matches
ORDER BY row_count DESC NULLS FIRST, event_type_len NULLS LAST
