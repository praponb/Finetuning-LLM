WITH cleaned AS (
    SELECT
        *,
        COALESCE(page, 'N/A') AS page_filled,
        SUBSTRING(page, 1, 3) AS page_prefix,
        COALESCE(page, 'OTHER') AS page_clean
    FROM web_events
    WHERE event_time >= DATE '2023-08-04'
)
SELECT
    id,
    duration_sec,
    page_prefix,
    page_clean,
    STDDEV_SAMP(duration_sec) OVER (PARTITION BY event_type) AS event_type_stddev_samp_duration_sec,
    SUM(duration_sec) OVER (PARTITION BY MONTH(event_time), event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_sum_90d
FROM cleaned
