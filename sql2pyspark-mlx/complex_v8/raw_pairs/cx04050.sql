WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('reopened', 'resolved')
),
summary AS (
    -- Group-level totals
    SELECT
        event_type,
        YEAR(event_at) AS event_at_year,
        COUNT(*) FILTER (WHERE event_type = 'assigned') AS n_event_type_assigned,
        COUNT(*) FILTER (WHERE event_type <> 'reopened') AS n_event_type_reopened,
        MIN(event_at) AS first_event_at,
        COUNT(*) AS cnt
    FROM cleaned_events
    WHERE actor = 'customer'
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(cnt - AVG(cnt) OVER w_event_type, 2) AS cnt_vs_event_type_avg,
        NTILE(4) OVER w_event_at_year_event_type_by AS cnt_quartile,
        DENSE_RANK() OVER (PARTITION BY event_at_year ORDER BY n_event_type_reopened) AS n_event_type_reopened_dense_rank,
        MAX(cnt) OVER (PARTITION BY event_type) AS event_type_max_cnt,
        ROUND(PERCENT_RANK() OVER w_event_at_year_event_type_by_2, 3) AS cnt_percent_rank,
        LEAD(cnt, 1, 0) OVER (PARTITION BY event_at_year, event_type ORDER BY cnt DESC, event_type, event_at_year) AS next_cnt
    FROM summary
    WINDOW w_event_type AS (PARTITION BY event_type), w_event_at_year_event_type_by AS (PARTITION BY event_at_year, event_type ORDER BY cnt DESC, event_type, event_at_year), w_event_at_year_event_type_by_2 AS (PARTITION BY event_at_year, event_type ORDER BY cnt DESC)
)
SELECT
    event_type,
    n_event_type_assigned,
    cnt,
    cnt_vs_event_type_avg,
    cnt_quartile,
    n_event_type_reopened_dense_rank,
    event_type_max_cnt,
    cnt_percent_rank,
    next_cnt,
    ABS(next_cnt - 8) AS next_cnt_distance
FROM sequenced
ORDER BY cnt_percent_rank DESC, event_type_max_cnt DESC
