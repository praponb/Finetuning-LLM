WITH base AS (
    SELECT
        id,
        session_id,
        page,
        event_time,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        COALESCE(page, 'OTHER') AS page_clean
    FROM web_events
    WHERE event_time > TO_DATE('2023-03-25')
),
metrics AS (
    SELECT
        id,
        session_id,
        page,
        event_time,
        page_clean,
        AVG(duration_sec_filled) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS duration_sec_filled_avg_5,
        STDDEV_SAMP(duration_sec_filled) OVER (PARTITION BY page) AS page_stddev_samp_duration_sec_filled,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM base
    WINDOW w_ordered AS (ORDER BY event_time DESC, id)
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS latest_rank
        FROM metrics
    ) ranked
    WHERE latest_rank = 1
),
gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM deduped
),
islands AS (
    SELECT
        id,
        page,
        event_time,
        duration_sec_filled_avg_5,
        page_stddev_samp_duration_sec_filled,
        SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_filled_avg_5 DESC, id) AS rn
    FROM islands
    QUALIFY rn = 1
),
kept_keys AS (
    SELECT id, page
    FROM ranked
    EXCEPT
    SELECT id, page
    FROM web_events
    WHERE event_time >= '2024-05-10'
),
reconciled AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.id = k.id AND s.page = k.page
),
base_web_events AS (
    SELECT
        id,
        page,
        event_time,
        duration_sec_filled_avg_5,
        page_stddev_samp_duration_sec_filled,
        island_id,
        rn,
        DATE_TRUNC('MONTH', event_time) AS event_time_period
    FROM reconciled
    WHERE page NOT IN ('/search', '/product', '/home')
      AND page IS DISTINCT FROM '/cart'
),
stats AS (
    SELECT
        CASE WHEN page_stddev_samp_duration_sec_filled >= 61 THEN 'HIGH' ELSE 'LOW' END AS page_stddev_samp_duration_sec_filled_tier,
        CASE WHEN island_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS island_id_tier,
        AVG(page_stddev_samp_duration_sec_filled) FILTER (WHERE page = '/home') AS home_avg_page_stddev_samp_duration_sec_filled,
        SIZE(COLLECT_SET(page)) AS num_pages,
        (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
        SORT_ARRAY(COLLECT_SET(page)) AS page_list,
        COUNT(DISTINCT page) AS distinct_pages
    FROM base_web_events
    GROUP BY CASE WHEN page_stddev_samp_duration_sec_filled >= 61 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN island_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING distinct_pages > 1
)
SELECT
    page_stddev_samp_duration_sec_filled_tier,
    island_id_tier,
    span_minutes,
    page_list
FROM stats
ORDER BY island_id_tier, span_minutes DESC
