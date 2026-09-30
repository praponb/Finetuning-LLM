WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
)
SELECT
    listener_id,
    source,
    track_id,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(ms_played), 2) AS stddev_ms_played,
    SUM(CASE WHEN NOT is_skipped THEN 1 ELSE 0 END) AS n_not_is_skipped,
    SUM(CASE WHEN device IN ('speaker', 'car', 'tv') THEN ms_played ELSE 0 END) AS ms_played_device_in_speaker_car_tv
FROM cleaned_plays
GROUP BY GROUPING SETS ((listener_id, source, track_id), (listener_id), (source))
ORDER BY listener_id NULLS LAST, source NULLS LAST, track_id NULLS LAST
