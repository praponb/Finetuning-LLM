WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source
    FROM plays
    WHERE ms_played < 230798
      AND listener_id <= 17
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
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        a.debut_year AS artist_debut_year
    FROM valid_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(track_duration_sec) OVER (PARTITION BY source, track_id), 2) AS source_track_id_avg_track_duration_sec,
        FIRST_VALUE(artist_debut_year) IGNORE NULLS OVER (ORDER BY track_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_artist_debut_year,
        MAX(artist_debut_year) OVER (PARTITION BY track_id) AS track_id_max_artist_debut_year,
        ROW_NUMBER() OVER w_track_id_is_skipped_by_track AS rn,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY listener_id, device), 2) AS artist_debut_year_vs_listener_id_device_avg,
        LAG(track_duration_sec) OVER (ORDER BY artist_debut_year DESC, id) AS prev_track_duration_sec,
        ROUND(track_duration_sec - AVG(track_duration_sec) OVER (PARTITION BY listener_id, device), 2) AS track_duration_sec_vs_listener_id_device_avg,
        ROUND(AVG(artist_debut_year) OVER (ORDER BY artist_debut_year DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS artist_debut_year_avg_4,
        ROUND(track_duration_sec - AVG(track_duration_sec) OVER (PARTITION BY source, track_id), 2) AS track_duration_sec_vs_source_track_id_avg,
        FIRST_VALUE(track_duration_sec) IGNORE NULLS OVER (ORDER BY track_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_track_duration_sec,
        COUNT(*) OVER w_source_track_id AS rows_in_source_track_id,
        FIRST_VALUE(artist_debut_year) OVER (PARTITION BY track_id, is_skipped ORDER BY track_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_debut_year,
        NTILE(5) OVER (ORDER BY track_duration_sec DESC, id) AS track_duration_sec_quintile,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER (PARTITION BY listener_id, device), 0), 4) AS track_duration_sec_share_of_listener_id_device
    FROM joined
    WINDOW w_track_id_is_skipped_by_track AS (PARTITION BY track_id, is_skipped ORDER BY track_duration_sec DESC, id), w_source_track_id AS (PARTITION BY source, track_id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        source,
        track_duration_sec,
        artist_debut_year,
        source_track_id_avg_track_duration_sec,
        first_known_artist_debut_year,
        track_id_max_artist_debut_year,
        rn,
        prev_track_duration_sec,
        track_duration_sec_vs_listener_id_device_avg,
        artist_debut_year_avg_4,
        track_duration_sec_vs_source_track_id_avg,
        first_known_track_duration_sec,
        rows_in_source_track_id,
        first_artist_debut_year,
        track_duration_sec_share_of_listener_id_device,
        MIN(artist_debut_year) OVER (PARTITION BY device) AS device_min_artist_debut_year,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY device)) AS distinct_source_in_device,
        MAX(first_artist_debut_year) OVER w_device_source AS device_source_max_first_artist_debut_year
    FROM with_trends
    WINDOW w_device_source AS (PARTITION BY device, source)
)
SELECT
    id,
    source,
    track_duration_sec,
    source_track_id_avg_track_duration_sec,
    first_known_artist_debut_year,
    track_id_max_artist_debut_year,
    track_duration_sec_vs_listener_id_device_avg,
    artist_debut_year_avg_4,
    track_duration_sec_vs_source_track_id_avg,
    first_artist_debut_year,
    device_min_artist_debut_year,
    device_source_max_first_artist_debut_year,
    COALESCE(track_duration_sec, 0) AS track_duration_sec_filled,
    COALESCE(prev_track_duration_sec, 0) AS prev_track_duration_sec_filled
FROM with_windows
ORDER BY track_duration_sec_vs_listener_id_device_avg DESC NULLS FIRST, device_source_max_first_artist_debut_year
