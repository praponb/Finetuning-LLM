WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        ROUND(duration_sec, 2) AS duration_sec_rounded,
        HOUR(event_time) AS event_time_hour
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-06-20 03:00:00'
      AND page IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_type,
        duration_sec,
        event_time_hour,
        LAST_VALUE(page) IGNORE NULLS OVER w_running AS last_known_page,
        AVG(duration_sec_rounded) OVER (PARTITION BY page) AS page_avg_duration_sec_rounded,
        AVG(duration_sec_rounded) OVER (ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_rounded_avg_7d,
        CUME_DIST() OVER w_recent AS duration_sec_rounded_cume_dist
    FROM base
    WHERE event_type = 'purchase'
    WINDOW w_running AS (PARTITION BY session_id ORDER BY event_time_hour ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY event_type ORDER BY duration_sec_rounded ASC)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        event_type,
        duration_sec,
        duration_sec_rounded_avg_7d,
        SUM(duration_sec_rounded_avg_7d) OVER (PARTITION BY event_type ORDER BY page_avg_duration_sec_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_duration_sec_rounded_avg_7d,
        AVG(duration_sec_rounded_avg_7d) OVER (PARTITION BY event_type ORDER BY page_avg_duration_sec_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_duration_sec_rounded_avg_7d,
        COUNT(duration_sec_rounded_avg_7d) OVER (PARTITION BY event_type ORDER BY page_avg_duration_sec_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_duration_sec_rounded_avg_7d,
        FIRST_VALUE(page_avg_duration_sec_rounded) IGNORE NULLS OVER (ORDER BY page_avg_duration_sec_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page_avg_duration_sec_rounded
    FROM enriched
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(running_avg_duration_sec_rounded_avg_7d) OVER (PARTITION BY event_type ORDER BY duration_sec_rounded_avg_7d ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_avg_duration_sec_rounded_avg_7d_avg_2,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY first_known_page_avg_duration_sec_rounded DESC, id ASC) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY first_known_page_avg_duration_sec_rounded DESC, id ASC) AS row_seq,
        AVG(running_count_duration_sec_rounded_avg_7d) OVER (PARTITION BY event_type ORDER BY running_avg_duration_sec_rounded_avg_7d ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_count_duration_sec_rounded_avg_7d_avg_3_prior,
        LAG(first_known_page_avg_duration_sec_rounded, 1) OVER (ORDER BY duration_sec_rounded_avg_7d ASC, id ASC) AS prev_first_known_page_avg_duration_sec_rounded
    FROM with_windows
),
with_windows_web_events AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY running_avg_duration_sec_rounded_avg_7d DESC) AS running_avg_duration_sec_rounded_avg_7d_percent_rank,
        FIRST_VALUE(running_avg_duration_sec_rounded_avg_7d) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY running_count_duration_sec_rounded_avg_7d_avg_3_prior ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_avg_duration_sec_rounded_avg_7d,
        FIRST_VALUE(duration_sec_rounded_avg_7d, TRUE) OVER (PARTITION BY event_type ORDER BY duration_sec_rounded_avg_7d ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec_rounded_avg_7d,
        COUNT(running_avg_duration_sec_rounded_avg_7d_avg_2) OVER (PARTITION BY event_type) AS event_type_count_running_avg_duration_sec_rounded_avg_7d_avg_2,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY running_count_duration_sec_rounded_avg_7d DESC, id ASC) AS row_seq_2
    FROM scored
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS dedup_rank
        FROM with_windows_web_events
    ) x
    WHERE dedup_rank = 1
),
with_windows_web_events_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        duration_sec,
        running_sum_duration_sec_rounded_avg_7d,
        running_avg_duration_sec_rounded_avg_7d,
        running_count_duration_sec_rounded_avg_7d,
        first_known_page_avg_duration_sec_rounded,
        row_seq,
        running_count_duration_sec_rounded_avg_7d_avg_3_prior,
        running_avg_duration_sec_rounded_avg_7d_percent_rank,
        first_known_duration_sec_rounded_avg_7d,
        dedup_rank,
        COUNT(duration_sec_rounded_avg_7d) OVER (PARTITION BY event_type ORDER BY first_known_duration_sec_rounded_avg_7d ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_duration_sec_rounded_avg_7d,
        SUM(duration_sec_rounded_avg_7d) OVER (PARTITION BY event_type ORDER BY first_known_duration_sec_rounded_avg_7d ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_duration_sec_rounded_avg_7d,
        AVG(duration_sec_rounded_avg_7d) OVER (PARTITION BY event_type ORDER BY first_known_duration_sec_rounded_avg_7d ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_duration_sec_rounded_avg_7d,
        NTILE(5) OVER (PARTITION BY event_type ORDER BY running_count_duration_sec_rounded_avg_7d_avg_3_prior ASC, id ASC) AS running_count_duration_sec_rounded_avg_7d_avg_3_prior_bucket
    FROM latest
)
-- Ranking and gap features
SELECT
    id,
    event_type,
    duration_sec,
    running_avg_duration_sec_rounded_avg_7d,
    first_known_page_avg_duration_sec_rounded,
    row_seq,
    running_avg_duration_sec_rounded_avg_7d_percent_rank,
    first_known_duration_sec_rounded_avg_7d,
    dedup_rank,
    last7_count_duration_sec_rounded_avg_7d,
    last7_sum_duration_sec_rounded_avg_7d,
    LAG(last7_avg_duration_sec_rounded_avg_7d) OVER (PARTITION BY event_type ORDER BY first_known_page_avg_duration_sec_rounded ASC, id ASC) AS prev_last7_avg_duration_sec_rounded_avg_7d,
    LEAD(running_avg_duration_sec_rounded_avg_7d) OVER (PARTITION BY event_type ORDER BY first_known_page_avg_duration_sec_rounded ASC, id ASC) AS next_running_avg_duration_sec_rounded_avg_7d,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY first_known_page_avg_duration_sec_rounded ASC, id ASC) AS seq_no,
    SUM(running_count_duration_sec_rounded_avg_7d) OVER (PARTITION BY event_type ORDER BY running_avg_duration_sec_rounded_avg_7d ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_count_duration_sec_rounded_avg_7d
FROM with_windows_web_events_2
