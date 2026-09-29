WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        event_type,
        page,
        event_time,
        CONCAT_WS('-', event_type, page) AS event_type_page_key,
        COALESCE(page, 'OTHER') AS page_filled
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-04-09 21:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        page,
        event_time,
        FIRST_VALUE(page) IGNORE NULLS OVER (PARTITION BY event_type, page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS row_seq
    FROM filtered
    WHERE page = '/search'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM with_windows
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_time,
        first_known_page,
        prev_event_time,
        RANK() OVER (ORDER BY burst_id) AS burst_id_rank,
        SUM(row_seq) OVER w_grp AS cumulative_row_seq,
        LAST_VALUE(page, TRUE) OVER (PARTITION BY page ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_page
    FROM islands
    WINDOW w_grp AS (PARTITION BY TO_DATE(prev_event_time), page ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN burst_id_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS burst_id_rank_tier,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    PERCENTILE_APPROX(burst_id_rank, ARRAY(0.25, 0.5, 0.75)) AS burst_id_rank_quartiles,
    COUNT(*) AS row_count
FROM enriched
GROUP BY burst_id_rank_tier, quarter_start
HAVING COUNT(*) > 1
ORDER BY burst_id_rank_tier NULLS LAST;
