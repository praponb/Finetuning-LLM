WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_2_prior,
        STDDEV_SAMP(duration_sec) OVER w_part AS page_stddev_samp_duration_sec
    FROM web_events
    WINDOW w_part AS (PARTITION BY page, event_type)
)
SELECT
    id,
    event_type,
    page,
    event_time,
    duration_sec,
    duration_sec_avg_2_prior,
    page_stddev_samp_duration_sec
FROM with_windows
ORDER BY event_type DESC;
