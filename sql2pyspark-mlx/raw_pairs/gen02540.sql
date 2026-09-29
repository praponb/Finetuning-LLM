WITH cleaned AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        CONCAT(event_type, ' ', page) AS event_type_page,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM web_events
    WHERE event_time > DATE '2023-07-07'
      AND page IS NOT NULL
),
flagged AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessions_web_events AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        event_type_page,
        page_filled,
        SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
metrics AS (
    SELECT
        id,
        page,
        event_type_page,
        page_filled,
        island_id,
        NTILE(10) OVER w_ordered AS island_id_bucket,
        FIRST_VALUE(page) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page,
        LAST_VALUE(page) IGNORE NULLS OVER (PARTITION BY page ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_page
    FROM sessions_web_events
    WHERE page = '/search'
    WINDOW w_ordered AS (PARTITION BY page ORDER BY island_id, id)
),
enriched AS (
    SELECT
        id,
        page,
        event_type_page,
        page_filled,
        island_id,
        island_id_bucket,
        last_known_page,
        NTILE(5) OVER (ORDER BY island_id, id) AS island_id_bucket_2,
        FIRST_VALUE(page) IGNORE NULLS OVER w_running AS first_known_page_2,
        NTILE(3) OVER (PARTITION BY page ORDER BY island_id_bucket, id) AS island_id_bucket_bucket
    FROM metrics
    WINDOW w_running AS (PARTITION BY page ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    page,
    page_filled,
    island_id_bucket,
    last_known_page,
    island_id_bucket_2,
    island_id_bucket_bucket
FROM enriched
