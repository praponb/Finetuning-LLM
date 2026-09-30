WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 10:00:00'
),
latest AS (
    -- Keep the latest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM cleaned
    ) ranked
    WHERE recency_rank = 1
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        event_type_len,
        COUNT(*) AS row_count,
        MIN(event_at) AS first_event_at,
        SUM(event_type_len) AS total_event_type_len,
        ROUND(STDDEV(event_type_len), 2) AS stddev_event_type_len
    FROM latest
    GROUP BY event_type_len
    HAVING COUNT(*) >= 2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        event_type_len,
        first_event_at,
        stddev_event_type_len,
        MIN(event_type_len) OVER w_first_event_at_to_date AS first_event_at_to_date_min_event_type_len,
        row_count - LAG(row_count) OVER (PARTITION BY row_count ORDER BY total_event_type_len, event_type_len) AS row_count_change,
        DENSE_RANK() OVER (PARTITION BY row_count ORDER BY total_event_type_len, event_type_len) AS total_event_type_len_dense_rank,
        LEAD(stddev_event_type_len, 1, 0) OVER w_row_count_by_total_event_typ AS next_stddev_event_type_len,
        NTILE(3) OVER (PARTITION BY row_count ORDER BY total_event_type_len, event_type_len) AS total_event_type_len_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY row_count ORDER BY total_event_type_len, event_type_len), 3) AS total_event_type_len_percent_rank
    FROM grouped
    WINDOW w_first_event_at_to_date AS (PARTITION BY TO_DATE(first_event_at)), w_row_count_by_total_event_typ AS (PARTITION BY row_count ORDER BY total_event_type_len, event_type_len)
),
derived AS (
    -- Derive helper columns
    SELECT
        event_type_len,
        stddev_event_type_len,
        first_event_at_to_date_min_event_type_len,
        row_count_change,
        next_stddev_event_type_len,
        FLOOR(row_count_change / 5) * 5 AS row_count_change_bucket,
        ABS(event_type_len - 6) AS event_type_len_distance
    FROM sequenced
    WHERE total_event_type_len_dense_rank <= 7
)
-- Final projection
SELECT
    event_type_len,
    stddev_event_type_len,
    first_event_at_to_date_min_event_type_len,
    row_count_change_bucket,
    event_type_len_distance
FROM derived
ORDER BY row_count_change_bucket DESC, stddev_event_type_len
