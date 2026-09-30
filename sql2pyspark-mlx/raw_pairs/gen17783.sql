WITH cleaned AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), event_time) / 365 years_since_event_time
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-03-22 05:00:00'
      AND page IS NOT NULL
)
SELECT
    id,
    session_id,
    event_type,
    event_time,
    duration_sec,
    years_since_event_time,
    STDDEV_SAMP(years_since_event_time) OVER (PARTITION BY page) page_stddev_samp_years_since_event_time,
    DENSE_RANK() OVER (PARTITION BY event_type ORDER BY years_since_event_time NULLS LAST) years_since_event_time_dense_rank
FROM cleaned;
