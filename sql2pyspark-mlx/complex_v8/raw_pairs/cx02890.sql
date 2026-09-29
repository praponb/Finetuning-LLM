WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type NOT IN ('reopened', 'escalated')
      AND actor NOT IN ('system', 'agent')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at ASC, id) AS prev_event_at,
        NTILE(3) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at NULLS LAST, id) AS event_at_tercile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, ticket_id ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at ASC, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        DENSE_RANK() OVER w_event_type_ticket_id_by_even_4 AS event_at_dense_rank,
        ROUND(PERCENT_RANK() OVER w_event_type_ticket_id_by_even_4, 3) AS event_at_percent_rank
    FROM recent_events
    WINDOW w_event_type_ticket_id_by_even_4 AS (PARTITION BY event_type, ticket_id ORDER BY event_at DESC)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        prev_event_at,
        event_at_tercile,
        minutes_since_prev_event_at,
        last_event_at,
        first_known_event_at,
        minutes_since_prev_event_at_2,
        event_at_percent_rank
    FROM windowed
    WHERE event_at_dense_rank = 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        prev_event_at,
        event_at_tercile,
        minutes_since_prev_event_at,
        last_event_at,
        first_known_event_at,
        minutes_since_prev_event_at_2,
        event_at_percent_rank,
        SUM(minutes_since_prev_event_at) OVER (ORDER BY event_at_percent_rank NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_minutes_since_prev_event_at,
        ROW_NUMBER() OVER w_event_type_by_event_at_terci AS seq_no,
        ROW_NUMBER() OVER w_all_by_event_at_percent_rank_2 AS seq_no_2,
        COUNT(*) OVER (PARTITION BY ticket_id, YEAR(event_at)) AS rows_in_ticket_id_event_at_year,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(first_known_event_at), event_at_tercile ORDER BY event_at_percent_rank DESC) AS event_at_percent_rank_dense_rank,
        DENSE_RANK() OVER w_all_by_first_known_event_at AS first_known_event_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(first_known_event_at), event_at_tercile ORDER BY event_at_percent_rank DESC, id) AS seq_no_4
    FROM leaders
    WINDOW w_event_type_by_event_at_terci AS (PARTITION BY event_type ORDER BY event_at_tercile ASC, id), w_all_by_event_at_percent_rank_2 AS (ORDER BY event_at_percent_rank NULLS LAST, id), w_all_by_first_known_event_at AS (ORDER BY first_known_event_at DESC)
),
totals AS (
    -- Group-level totals
    SELECT
        YEAR(event_at) AS event_at_year,
        DATE_TRUNC('MONTH', first_known_event_at) AS first_known_event_at_month,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        PERCENTILE_APPROX(event_at_tercile, 0.5) AS median_event_at_tercile,
        MIN(first_known_event_at_dense_rank) AS min_first_known_event_at_dense_rank
    FROM with_windows
    GROUP BY YEAR(event_at), DATE_TRUNC('MONTH', first_known_event_at)
    HAVING COUNT(*) >= 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(distinct_ids / NULLIF(SUM(distinct_ids) OVER (PARTITION BY min_first_known_event_at_dense_rank, TO_DATE(first_known_event_at_month)), 0), 4) AS distinct_ids_share_of_min_first_known_event_at_d,
        event_at_year - LAG(event_at_year) OVER (PARTITION BY min_first_known_event_at_dense_rank, event_at_year ORDER BY cnt ASC, event_at_year, first_known_event_at_month) AS event_at_year_change
    FROM totals
)
SELECT
    event_at_year,
    first_known_event_at_month,
    distinct_ids,
    cnt,
    min_first_known_event_at_dense_rank,
    ABS(median_event_at_tercile - 4) AS median_event_at_tercile_distance,
    COALESCE(cnt, 0) AS cnt_filled
FROM metrics
ORDER BY event_at_year
