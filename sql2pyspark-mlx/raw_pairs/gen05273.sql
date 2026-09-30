WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(page, 'Unknown') AS page_filled
    FROM web_events
    WHERE page IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time, id) AS row_seq
    FROM cleaned
),
scored_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY session_id, page ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_dense_rank,
        CUME_DIST() OVER (ORDER BY duration_sec NULLS FIRST) AS duration_sec_cume_dist,
        SUM(running_count_duration_sec) OVER w_hist AS cumulative_running_count_duration_sec,
        COUNT(duration_sec) OVER w_part AS event_type_count_duration_sec
    FROM scored
    WHERE event_type IN ('purchase', 'page_view')
    WINDOW w_hist AS (PARTITION BY page, event_type ORDER BY running_max_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY event_type)
),
with_dims AS (
    -- Attach sessions attributes
    SELECT src.*, b.country
    FROM scored_web_events src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_dims
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
matched_keys AS (
    -- Set intersection on keys
    SELECT page, id
    FROM sessions_web_events
    INTERSECT
    SELECT page, id
    FROM web_events
    WHERE session_id IN (8, 12, 18, 31)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_web_events s
    INNER JOIN matched_keys k ON s.page = k.page AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    session_id,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    MAX(event_time) AS ended_at,
    COUNT(*) AS num_rows
FROM kept_rows
GROUP BY session_id, activity_month, year_start
ORDER BY session_id DESC NULLS LAST;
