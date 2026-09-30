WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label_2
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 7 AND 13
      AND event_type <> 'escalated'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_ref,
        event_at_label,
        event_at_label_2,
        NTILE(5) OVER (PARTITION BY actor ORDER BY event_at, id) AS event_at_quintile,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at), 3) AS event_at_percent_rank,
        LEAD(event_at, 3) OVER w_all_by_event_at AS next3_event_at
    FROM prepared
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id)
),
event_type_stats AS (
    -- Avg event_at_quintile per event_type
    SELECT event_type, AVG(event_at_quintile) AS avg_event_at_quintile
    FROM with_windows
    GROUP BY event_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_at_quintile,
        ROUND(s.event_at_quintile - g.avg_event_at_quintile, 2) AS event_at_quintile_minus_avg
    FROM with_windows s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.event_at_quintile > g.avg_event_at_quintile
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(avg_event_at_quintile) OVER (PARTITION BY ticket_id, YEAR(event_at) ORDER BY event_at_quintile_minus_avg, id) AS next_avg_event_at_quintile,
        LEAD(event_at_quintile, 3) OVER (PARTITION BY ticket_id, YEAR(event_at) ORDER BY event_at_quintile_minus_avg, id) AS next3_event_at_quintile
    FROM above_average
),
with_windows_3 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor_ref,
        event_at_label,
        event_at_label_2,
        event_at_quintile,
        next3_event_at,
        next_avg_event_at_quintile,
        next3_event_at_quintile,
        ROUND(CUME_DIST() OVER w_all_by_next_avg_event_at_qui, 3) AS next_avg_event_at_quintile_cume_dist,
        COUNT(*) OVER (PARTITION BY next3_event_at_quintile, event_type) AS rows_in_next3_event_at_quintile_event_type,
        FIRST_VALUE(next_avg_event_at_quintile) IGNORE NULLS OVER (PARTITION BY YEAR(event_at), event_at_quintile ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_avg_event_at_quintile,
        ROUND(PERCENT_RANK() OVER w_next3_event_at_year_by_next3, 3) AS next3_event_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(event_at), event_at_quintile ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        ROW_NUMBER() OVER w_next3_event_at_year_by_next3_2 AS rn
    FROM with_windows_2
    WINDOW w_all_by_next_avg_event_at_qui AS (ORDER BY next_avg_event_at_quintile), w_next3_event_at_year_by_next3 AS (PARTITION BY YEAR(next3_event_at) ORDER BY next3_event_at DESC), w_next3_event_at_year_by_next3_2 AS (PARTITION BY YEAR(next3_event_at) ORDER BY next3_event_at DESC, id)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_at_label_2,
        next_avg_event_at_quintile,
        next3_event_at_quintile,
        next_avg_event_at_quintile_cume_dist,
        rows_in_next3_event_at_quintile_event_type,
        first_known_next_avg_event_at_quintile,
        next3_event_at_percent_rank,
        event_at_cume_dist,
        rn,
        COALESCE(next3_event_at_quintile, 0) AS next3_event_at_quintile_filled,
        YEAR(next3_event_at) AS next3_event_at_year
    FROM with_windows_3
    WHERE first_known_next_avg_event_at_quintile > 3.7
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_at_label_2,
        next_avg_event_at_quintile,
        next3_event_at_quintile,
        rows_in_next3_event_at_quintile_event_type,
        first_known_next_avg_event_at_quintile,
        event_at_cume_dist,
        rn,
        next3_event_at_year,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM derived
    WHERE HOUR(event_at) BETWEEN 6 AND 11
)
-- Final projection
SELECT
    id,
    event_at,
    event_at_label_2,
    next_avg_event_at_quintile,
    rows_in_next3_event_at_quintile_event_type,
    first_known_next_avg_event_at_quintile,
    event_at_cume_dist,
    rn,
    event_at_label,
    SUBSTRING(TRIM(event_at_label), 1, 3) AS event_at_label_prefix,
    DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
FROM labeled
