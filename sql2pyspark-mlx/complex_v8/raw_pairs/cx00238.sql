WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE YEAR(release_date) = 2024
      AND artist_id IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(duration_sec / NULLIF(moods_count, 0), 3) AS duration_sec_per_moods_count,
        duration_sec - moods_count AS duration_sec_minus_moods_count,
        moods_count + duration_sec AS moods_count_plus_duration_sec
    FROM valid_tracks
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) AS track_count,
        MIN(duration_sec) AS min_track_duration_sec,
        MIN(release_date) AS first_track_release_date
    FROM tracks
    GROUP BY artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count,
        t.min_track_duration_sec,
        t.first_track_release_date
    FROM enriched s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY artist_id, track_count ORDER BY moods_count_plus_duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY artist_id, track_count ORDER BY moods_count_plus_duration_sec DESC NULLS FIRST, id) AS moods_count_plus_duration_sec_seq,
        SUM(min_track_duration_sec) OVER (ORDER BY min_track_duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS min_track_duration_sec_sum_6_prior,
        SUM(duration_sec) OVER (ORDER BY min_track_duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_2_prior
    FROM summarized_rows
)
SELECT
    moods_count,
    is_explicit,
    GROUPING(moods_count) AS is_total_moods_count,
    ROUND(SUM(min_track_duration_sec) / NULLIF(SUM(duration_sec), 0), 3) AS min_track_duration_sec_to_duration_sec_ratio
FROM with_windows
GROUP BY CUBE(moods_count, is_explicit)
