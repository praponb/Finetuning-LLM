WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE MONTH(played_at) IN (1, 2)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY source_len, track_id ORDER BY played_at NULLS LAST, id) AS played_at_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY source_len, track_id ORDER BY played_at NULLS LAST, id), 3) AS played_at_cume_dist,
        RANK() OVER w_source_len_track_id_by_playe_2 AS played_at_rank,
        ROUND(source_len - AVG(source_len) OVER w_device, 2) AS source_len_vs_device_avg,
        MAX(source_len) OVER w_device AS device_max_source_len,
        NTILE(10) OVER (PARTITION BY source ORDER BY played_at ASC, id) AS played_at_decile,
        MAX(source_len) OVER (PARTITION BY source ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_source_len_so_far,
        RANK() OVER (PARTITION BY source, listener_id ORDER BY source_len) AS source_len_rank,
        NTILE(3) OVER w_source_len_track_id_by_playe AS played_at_tercile,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY source_len, track_id ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        SIZE(COLLECT_SET(track_id) OVER w_device) AS distinct_track_id_in_device,
        ROUND(AVG(source_len) OVER (PARTITION BY source_len, listener_id ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS source_len_avg_3_prior,
        ROUND(AVG(source_len) OVER (PARTITION BY source ORDER BY played_at ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS source_len_avg_2,
        ROUND(CUME_DIST() OVER (PARTITION BY source, listener_id ORDER BY source_len, id), 3) AS source_len_cume_dist
    FROM cleaned_plays
    WINDOW w_source_len_track_id_by_playe_2 AS (PARTITION BY source_len, track_id ORDER BY played_at NULLS LAST), w_device AS (PARTITION BY device), w_source_len_track_id_by_playe AS (PARTITION BY source_len, track_id ORDER BY played_at NULLS LAST, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        source_len,
        played_at_quartile,
        played_at_cume_dist,
        played_at_rank,
        source_len_vs_device_avg,
        device_max_source_len,
        played_at_decile,
        max_source_len_so_far,
        played_at_tercile,
        second_played_at,
        distinct_track_id_in_device,
        source_len_avg_3_prior,
        source_len_avg_2,
        source_len_cume_dist
    FROM with_trends
    WHERE source_len_rank <= 5
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    source,
    played_at_quartile,
    played_at_cume_dist,
    device_max_source_len,
    played_at_decile,
    max_source_len_so_far,
    played_at_tercile,
    second_played_at,
    distinct_track_id_in_device,
    source_len_avg_3_prior,
    source_len_avg_2,
    source_len_cume_dist,
    ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(second_played_at)) / 3600, 1) AS hours_second_played_at_to_played_at,
    CONCAT(source, '#', CAST(device_max_source_len AS STRING)) AS source_ref
FROM top_per_group
ORDER BY played_at, id
LIMIT 10
