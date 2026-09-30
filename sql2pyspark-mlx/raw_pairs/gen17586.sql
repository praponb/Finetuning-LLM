WITH base AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        COALESCE(page, 'NONE') AS resolved_page
    FROM web_events
    WHERE event_time >= DATE '2023-04-15'
      AND page IS NOT NULL
),
gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
sessionized AS (
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
metrics AS (
    SELECT
        id,
        session_no,
        SUM(duration_sec) OVER (PARTITION BY TO_DATE(event_time) ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        LEAD(session_no, 2) OVER w_recent AS next_session_no,
        SUM(session_no) OVER w_ordered AS running_session_no,
        MIN(duration_sec) OVER w_recent_2 AS page_min_duration_sec,
        AVG(session_no) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS session_no_avg_90d
    FROM sessionized
    WINDOW w_recent AS (PARTITION BY DATE_TRUNC('MONTH', event_time), page ORDER BY event_time, id), w_ordered AS (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent_2 AS (PARTITION BY page)
),
metrics_web_events AS (
    SELECT
        *,
        FIRST_VALUE(page_min_duration_sec, TRUE) OVER (ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page_min_duration_sec,
        LEAD(session_no_avg_90d, 1) OVER (ORDER BY session_no, id) AS next_session_no_avg_90d,
        AVG(next_session_no) OVER (ORDER BY session_no, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS next_session_no_avg_6_prior
    FROM metrics
),
scored AS (
    SELECT
        id,
        session_no,
        running_duration_sec,
        running_session_no,
        next_session_no_avg_6_prior,
        COUNT(session_no_avg_90d) OVER (ORDER BY next_session_no_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_session_no_avg_90d,
        MIN(session_no_avg_90d) OVER (ORDER BY next_session_no_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_session_no_avg_90d,
        MAX(session_no_avg_90d) OVER (ORDER BY next_session_no_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_session_no_avg_90d
    FROM metrics_web_events
),
metrics_web_events_2 AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY session_no) AS session_no_percent_rank,
        LAST_VALUE(running_count_session_no_avg_90d) IGNORE NULLS OVER w_part AS last_known_running_count_session_no_avg_90d,
        SUM(running_max_session_no_avg_90d) OVER w_running AS cumulative_running_max_session_no_avg_90d
    FROM scored
    WINDOW w_part AS (ORDER BY next_session_no_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (ORDER BY running_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        id,
        running_duration_sec,
        running_session_no,
        next_session_no_avg_6_prior,
        running_count_session_no_avg_90d,
        running_min_session_no_avg_90d,
        running_max_session_no_avg_90d,
        session_no_percent_rank,
        last_known_running_count_session_no_avg_90d,
        AVG(running_session_no) OVER (ORDER BY next_session_no_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_running_session_no,
        COUNT(running_session_no) OVER (ORDER BY next_session_no_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_running_session_no,
        SUM(running_session_no) OVER (ORDER BY next_session_no_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_running_session_no,
        MIN(running_session_no) OVER (ORDER BY next_session_no_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_running_session_no,
        AVG(session_no) OVER (ORDER BY running_min_session_no_avg_90d, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS session_no_avg_6_prior
    FROM metrics_web_events_2
)
SELECT
    id,
    kpi,
    val
FROM (
    SELECT
        id,
        running_max_session_no_avg_90d,
        session_no_percent_rank,
        last4_avg_running_session_no
    FROM enriched
) src
UNPIVOT (
    val FOR kpi IN (running_max_session_no_avg_90d, session_no_percent_rank, last4_avg_running_session_no)
);
