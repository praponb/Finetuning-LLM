WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'focus')
      AND duration_sec > 239
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        artist_id,
        SUM(CASE WHEN UPPER(TRIM(title)) LIKE 'G%' THEN 1 ELSE 0 END) AS n_upper_trim_title_like_g,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        ROUND(SUM(bpm) / NULLIF(SUM(duration_sec), 0), 3) AS bpm_to_duration_sec_ratio,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list
    FROM prepared_tracks
    GROUP BY DATE_FORMAT(release_date, 'yyyy-MM'), artist_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        release_date_ym,
        artist_id,
        n_upper_trim_title_like_g,
        bpm_to_duration_sec_ratio,
        title_list,
        UPPER(TRIM(release_date_ym)) AS release_date_ym_clean,
        COALESCE(distinct_artist_ids, 0) AS distinct_artist_ids_filled
    FROM summary
    WHERE distinct_artist_ids BETWEEN 1 AND 9
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre
    FROM annotated s
    JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        release_date_ym,
        artist_id,
        n_upper_trim_title_like_g,
        bpm_to_duration_sec_ratio,
        title_list,
        release_date_ym_clean,
        artist_is_verified,
        artist_debut_year,
        ROUND(bpm_to_duration_sec_ratio / NULLIF(SUM(bpm_to_duration_sec_ratio) OVER w_distinct_artist_ids_filled, 0), 4) AS bpm_to_duration_sec_ratio_share_of_distinct_arti,
        MAX(distinct_artist_ids_filled) OVER (PARTITION BY artist_id ORDER BY bpm_to_duration_sec_ratio DESC NULLS FIRST, release_date_ym, artist_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS distinct_artist_ids_filled_max_2_prior
    FROM with_parent_artist
    WINDOW w_distinct_artist_ids_filled AS (PARTITION BY distinct_artist_ids_filled)
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) AS track_count,
        MIN(release_date) AS first_track_release_date,
        COUNT(CASE WHEN is_explicit = TRUE THEN 1 END) AS track_is_explicit_count
    FROM tracks
    GROUP BY artist_id
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count,
        t.first_track_release_date,
        COALESCE(t.track_is_explicit_count, 0) AS track_is_explicit_count
    FROM metrics s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
)
-- Final projection
SELECT
    release_date_ym,
    n_upper_trim_title_like_g,
    bpm_to_duration_sec_ratio,
    title_list,
    release_date_ym_clean,
    artist_debut_year,
    bpm_to_duration_sec_ratio_share_of_distinct_arti,
    distinct_artist_ids_filled_max_2_prior,
    track_count,
    first_track_release_date,
    track_is_explicit_count
FROM profiled
ORDER BY release_date_ym_clean DESC
