WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE listener_id <= 18
      AND source = 'album'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        ROW_NUMBER() OVER (ORDER BY played_at, id) AS seq_no,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        NTILE(10) OVER w_device_by_played_at AS played_at_decile,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_2
    FROM recent_plays
    WINDOW w_device_by_played_at AS (PARTITION BY device ORDER BY played_at DESC, id)
)
SELECT
    track_id,
    is_skipped,
    GROUPING(track_id) AS is_total_track_id,
    SUM(CASE WHEN device IN ('phone', 'speaker', 'car') THEN 1 ELSE 0 END) AS n_device_in_phone_speaker_car
FROM with_trends
GROUP BY GROUPING SETS ((track_id, is_skipped), (track_id))
ORDER BY track_id NULLS LAST, is_skipped NULLS LAST
