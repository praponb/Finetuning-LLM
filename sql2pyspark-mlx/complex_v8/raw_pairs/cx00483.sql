WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
    WHERE is_skipped = FALSE
      AND ms_played < 324058
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        s.played_at_label,
        s.days_since_played_at,
        t.bpm AS track_bpm,
        t.artist_id AS track_artist_id
    FROM scoped_plays s
    JOIN tracks t ON t.id = s.track_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(days_since_played_at) OVER (PARTITION BY track_artist_id, is_skipped) AS track_artist_id_is_skipped_max_days_since_played,
        LAG(track_bpm, 3) OVER (PARTITION BY device, track_artist_id ORDER BY track_bpm DESC, id) AS prev3_track_bpm,
        FIRST_VALUE(track_bpm) IGNORE NULLS OVER (PARTITION BY listener_id, device ORDER BY track_bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_track_bpm,
        LAST_VALUE(track_bpm) IGNORE NULLS OVER (PARTITION BY device, track_artist_id ORDER BY track_bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_track_bpm,
        RANK() OVER (PARTITION BY listener_id, device ORDER BY track_bpm DESC NULLS FIRST) AS track_bpm_rank,
        MIN(days_since_played_at) OVER (ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_days_since_played_at_so_far,
        track_bpm - LAG(track_bpm) OVER (ORDER BY track_bpm, id) AS track_bpm_change,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY track_artist_id, is_skipped)) AS distinct_device_in_track_artist_id_is_skipped,
        COUNT(*) OVER w_track_artist_id_is_skipped AS rows_in_track_artist_id_is_skipped,
        SUM(track_bpm) OVER (PARTITION BY device, track_artist_id ORDER BY track_bpm DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS track_bpm_sum_2_prior,
        NTH_VALUE(track_bpm, 2) OVER (PARTITION BY listener_id, device ORDER BY track_bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_bpm,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY track_artist_id, is_skipped)) AS distinct_track_id_in_track_artist_id_is_skipped,
        SUM(days_since_played_at) OVER (PARTITION BY track_artist_id, is_skipped) AS track_artist_id_is_skipped_sum_days_since_played
    FROM joined
    WINDOW w_track_artist_id_is_skipped AS (PARTITION BY track_artist_id, is_skipped)
),
tagged AS (
    -- Derive helper columns
    SELECT *, COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM ranked
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    source,
    played_at_label,
    track_bpm,
    track_artist_id_is_skipped_max_days_since_played,
    prev3_track_bpm,
    first_known_track_bpm,
    last_known_track_bpm,
    track_bpm_rank,
    track_bpm_change,
    distinct_device_in_track_artist_id_is_skipped,
    rows_in_track_artist_id_is_skipped,
    track_bpm_sum_2_prior,
    second_track_bpm,
    distinct_track_id_in_track_artist_id_is_skipped,
    track_artist_id_is_skipped_sum_days_since_played,
    device_or_unknown,
    ROUND(days_since_played_at / NULLIF(track_bpm_sum_2_prior, 0), 3) AS days_since_played_at_per_track_bpm_sum_2_prior,
    LENGTH(TRIM(source)) AS source_len
FROM tagged
ORDER BY track_id
