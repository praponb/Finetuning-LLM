WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        CONCAT(event_type, ', ', page) AS event_type_page,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-06-07 20:00:00'
      AND page IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        event_type_page,
        page_filled,
        LOWER(event_type) AS event_type_lower,
        COALESCE(page, 'N/A') AS page_clean
    FROM cleaned
    WHERE event_time >= DATE '2023-09-30'
      AND page IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY event_time, id) AS row_seq
    FROM filtered
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(seq_no) OVER (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        SUM(seq_no) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no_2,
        AVG(row_seq) OVER (ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS row_seq_avg_6,
        MIN(row_seq) OVER (PARTITION BY event_type) AS event_type_min_row_seq
    FROM with_windows
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.event_time,
    s.event_type_page,
    s.page_clean,
    s.seq_no,
    s.row_seq_avg_6,
    s.event_type_min_row_seq,
    (SELECT AVG(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM metrics s
