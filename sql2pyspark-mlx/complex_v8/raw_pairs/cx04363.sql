WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        LOWER(TRIM(device)) AS device_clean
    FROM plays
    WHERE ms_played <= 214343
      AND is_skipped = TRUE
),
enriched AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device_clean,
        t.artist_id AS track_artist_id,
        t.duration_sec AS track_duration_sec,
        t.is_explicit AS track_is_explicit
    FROM filtered_plays s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.duration_sec > 144
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        track_artist_id,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id ORDER BY track_duration_sec ASC, id), 3) AS track_duration_sec_cume_dist,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER (PARTITION BY track_is_explicit), 0), 4) AS track_duration_sec_share_of_track_is_explicit,
        ROUND(CUME_DIST() OVER (PARTITION BY track_artist_id, listener_id ORDER BY track_duration_sec DESC, id), 3) AS track_duration_sec_cume_dist_2,
        LAST_VALUE(track_duration_sec) IGNORE NULLS OVER (PARTITION BY track_is_explicit, track_artist_id ORDER BY track_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_track_duration_sec,
        ROUND(AVG(track_duration_sec) OVER (PARTITION BY track_is_explicit), 2) AS track_is_explicit_avg_track_duration_sec,
        RANK() OVER (PARTITION BY listener_id ORDER BY track_duration_sec ASC) AS track_duration_sec_rank,
        MAX(track_duration_sec) OVER (PARTITION BY track_artist_id ORDER BY track_duration_sec DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS track_duration_sec_max_6_prior,
        COUNT(*) OVER w_track_artist_id AS rows_in_track_artist_id,
        DENSE_RANK() OVER (PARTITION BY track_is_explicit ORDER BY track_duration_sec ASC) AS track_duration_sec_dense_rank,
        LAG(track_duration_sec, 1, 0) OVER (PARTITION BY track_artist_id, listener_id ORDER BY track_duration_sec DESC, id) AS prev_track_duration_sec
    FROM enriched
    WINDOW w_track_artist_id AS (PARTITION BY track_artist_id)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(track_duration_sec_rank / 2) * 2 AS track_duration_sec_rank_bucket,
        ROUND(track_duration_sec_rank / NULLIF(rows_in_track_artist_id, 0), 3) AS track_duration_sec_rank_per_rows_in_track_artist,
        COALESCE(rows_in_track_artist_id, 0) AS rows_in_track_artist_id_filled
    FROM metrics
    WHERE track_is_explicit_avg_track_duration_sec <= 321.0
)
SELECT
    id,
    track_id,
    track_artist_id,
    track_duration_sec_cume_dist,
    track_duration_sec_cume_dist_2,
    last_known_track_duration_sec,
    track_is_explicit_avg_track_duration_sec,
    track_duration_sec_rank,
    track_duration_sec_max_6_prior,
    rows_in_track_artist_id,
    prev_track_duration_sec,
    track_duration_sec_rank_per_rows_in_track_artist,
    rows_in_track_artist_id_filled,
    rows_in_track_artist_id + prev_track_duration_sec AS rows_in_track_artist_id_plus_prev_track_duration,
    COALESCE(prev_track_duration_sec, 0) AS prev_track_duration_sec_filled
FROM scored
ORDER BY track_duration_sec_max_6_prior DESC
