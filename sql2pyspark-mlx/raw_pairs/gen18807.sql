WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_duration_sec,
        PERCENT_RANK() OVER (ORDER BY duration_sec NULLS LAST) duration_sec_percent_rank,
        LEAD(duration_sec, 2) OVER (PARTITION BY session_id ORDER BY event_time, id) next_duration_sec,
        MAX(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) duration_sec_max_7d
    FROM web_events
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(next_duration_sec) OVER w_hist running_sum_next_duration_sec,
        AVG(next_duration_sec) OVER w_hist running_avg_next_duration_sec,
        MAX(next_duration_sec) OVER w_hist running_max_next_duration_sec
    FROM sessionized
    WINDOW w_hist AS (PARTITION BY event_type ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(running_avg_next_duration_sec) OVER (PARTITION BY session_id ORDER BY running_max_next_duration_sec, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) running_avg_next_duration_sec_avg_5,
        AVG(duration_sec_percent_rank) OVER (PARTITION BY session_id ORDER BY duration_sec_max_7d, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) duration_sec_percent_rank_avg_5_prior
    FROM metrics
    WHERE running_sum_next_duration_sec <= 2000
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(duration_sec_max_7d, 0) duration_sec_max_7d_filled,
        DATE_TRUNC('WEEK', event_time) event_time_period
    FROM metrics_web_events
    WHERE event_time > TO_DATE('2023-10-18')
      AND page IS NOT NULL
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            session_id,
            event_type,
            running_sum_next_duration_sec
        FROM cleaned
    ) src
    PIVOT (
        AVG(running_sum_next_duration_sec)
        FOR event_type IN ('search' AS search, 'page_view' AS page_view, 'add_to_cart' AS add_to_cart)
    )
)
SELECT session_id, page_view
FROM wide;
