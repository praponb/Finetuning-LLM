WITH base AS (
    SELECT
        *,
        LENGTH(page) AS page_length,
        COALESCE(page, 'OTHER') AS page_clean
    FROM web_events
    WHERE event_type = 'click'
),
enriched AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY event_type ORDER BY page_length DESC, id) AS page_length_bucket,
        LAST_VALUE(page, TRUE) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_page,
        AVG(duration_sec) OVER (PARTITION BY MONTH(event_time), session_id ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_6_prior
    FROM base
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM enriched
),
sessions_web_events AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        page_clean,
        SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
metrics AS (
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        page_clean,
        island_id,
        AVG(duration_sec) OVER (PARTITION BY event_type, page ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_2_prior,
        NTILE(3) OVER (ORDER BY duration_sec, id) AS duration_sec_bucket
    FROM sessions_web_events
),
scored AS (
    SELECT
        *,
        SUM(duration_sec_avg_2_prior) OVER w_ordered AS cumulative_duration_sec_avg_2_prior,
        NTILE(3) OVER w_ordered_2 AS island_id_bucket,
        AVG(island_id) OVER (PARTITION BY TO_DATE(event_time), session_id ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS island_id_avg_6_prior,
        AVG(duration_sec_bucket) OVER (PARTITION BY session_id ORDER BY island_id, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_bucket_avg_2,
        NTILE(3) OVER w_hist AS duration_sec_bucket_bucket
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY session_id, event_type ORDER BY duration_sec_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered_2 AS (ORDER BY island_id DESC, id), w_hist AS (ORDER BY duration_sec_bucket DESC, id)
)
SELECT
    p.device,
    src.event_type,
    COUNT(*) AS rows_total,
    ROUND(AVG(src.duration_sec_avg_2_prior), 2) AS avg_duration_sec_avg_2_prior,
    AVG(src.island_id) AS avg_island_id,
    GROUPING_ID() AS level_id
FROM scored src
JOIN sessions p ON src.session_id = p.id
GROUP BY GROUPING SETS ((p.device, src.event_type), (p.device), ())
