WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        TRIM(page) AS clean_page,
        DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time
    FROM web_events
    WHERE event_time >= TO_TIMESTAMP('2023-04-16 07:00:00')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        clean_page
    FROM (
        SELECT *, RANK() OVER (PARTITION BY event_type, session_id ORDER BY event_time DESC NULLS FIRST) AS row_num
        FROM cleaned
    ) x
    WHERE row_num = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_type,
        page,
        duration_sec,
        clean_page,
        ROW_NUMBER() OVER (ORDER BY duration_sec ASC, id ASC) AS seq_no,
        AVG(duration_sec) OVER (PARTITION BY YEAR(event_time), page) AS event_time_year_avg_duration_sec,
        DENSE_RANK() OVER (PARTITION BY YEAR(event_time) ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_dense_rank
    FROM ranked
    WHERE page IN ('/search', '/product')
),
ranked_web_events AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_dense_rank DESC NULLS FIRST, id ASC) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        page,
        seq_no,
        LAG(duration_sec_dense_rank) OVER w_part AS prev_duration_sec_dense_rank,
        LEAD(duration_sec) OVER w_part AS next_duration_sec,
        ROW_NUMBER() OVER w_part AS row_seq,
        NTILE(5) OVER (PARTITION BY page ORDER BY event_time_year_avg_duration_sec ASC, id ASC) AS event_time_year_avg_duration_sec_bucket
    FROM ranked_web_events
    WINDOW w_part AS (PARTITION BY page ORDER BY duration_sec_dense_rank ASC, id ASC)
)
SELECT
    id,
    page,
    seq_no,
    prev_duration_sec_dense_rank,
    next_duration_sec,
    row_seq,
    event_time_year_avg_duration_sec_bucket
FROM metrics
ORDER BY event_time_year_avg_duration_sec_bucket DESC NULLS FIRST, seq_no ASC;
