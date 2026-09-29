WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        CAST(event_time AS DATE) AS event_time_day,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM web_events
    WHERE event_time >= TO_DATE('2023-06-21')
      AND page IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        event_time,
        duration_sec,
        COUNT(duration_sec_filled) OVER w_part AS running_count_duration_sec_filled,
        MIN(duration_sec_filled) OVER w_part AS running_min_duration_sec_filled,
        AVG(duration_sec_filled) OVER w_part AS running_avg_duration_sec_filled,
        SUM(duration_sec_filled) OVER w_part AS running_sum_duration_sec_filled
    FROM cleaned
    WHERE event_time_day > '2023-06-15'
    WINDOW w_part AS (PARTITION BY event_type ORDER BY duration_sec_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(running_avg_duration_sec_filled) OVER w_hist AS running_running_avg_duration_sec_filled,
        ROW_NUMBER() OVER w_hist_2 AS event_seq,
        AVG(running_sum_duration_sec_filled) OVER (ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_sum_duration_sec_filled_avg_2
    FROM scored
    WINDOW w_hist AS (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (ORDER BY running_count_duration_sec_filled DESC, id)
),
with_windows_web_events AS (
    -- Ranking and gap features
    SELECT
        id,
        event_time,
        running_count_duration_sec_filled,
        running_avg_duration_sec_filled,
        running_sum_duration_sec_filled,
        running_sum_duration_sec_filled_avg_2,
        NTILE(10) OVER (ORDER BY running_running_avg_duration_sec_filled DESC, id) AS running_running_avg_duration_sec_filled_bucket,
        AVG(running_sum_duration_sec_filled_avg_2) OVER (ORDER BY running_sum_duration_sec_filled_avg_2, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_sum_duration_sec_filled_avg_2_avg_3_prior
    FROM with_windows
)
SELECT
    id,
    running_avg_duration_sec_filled,
    running_sum_duration_sec_filled,
    running_running_avg_duration_sec_filled_bucket,
    running_sum_duration_sec_filled_avg_2_avg_3_prior
FROM with_windows_web_events
ORDER BY id DESC NULLS FIRST
LIMIT 20;
