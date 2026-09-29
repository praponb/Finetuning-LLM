WITH base AS (
    SELECT
        *,
        LOWER(page) AS page_lower,
        COALESCE(page, 'OTHER') AS page_clean
    FROM web_events
    WHERE event_time >= TO_DATE('2023-04-23')
      AND page IS NOT NULL
)
SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_dense_rank,
    AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_avg_7d,
    MAX(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_max_30d
FROM base
