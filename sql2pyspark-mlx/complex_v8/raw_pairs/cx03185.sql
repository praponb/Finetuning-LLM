WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'chill')
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at,
        (SELECT COUNT(CASE WHEN p.is_skipped = FALSE THEN 1 END) FROM plays p WHERE p.track_id = s.id) AS play_is_skipped_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at
    FROM prepared_tracks s
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        play_is_skipped_count,
        NTILE(5) OVER w_all_by_bpm AS bpm_quintile,
        NTH_VALUE(bpm, 2) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm
    FROM summarized_rows
    WINDOW w_all_by_bpm AS (ORDER BY bpm, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(bpm, 2) OVER (PARTITION BY artist_id ORDER BY bpm_quintile, id) AS prev2_bpm,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm_quintile, id) AS seq_no,
        NTH_VALUE(second_bpm, 2) OVER (PARTITION BY artist_id ORDER BY bpm_quintile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_bpm
    FROM metrics
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
qualified AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND UPPER(TRIM(a.artist_name)) LIKE 'K%'
        )
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    play_is_skipped_count,
    bpm_quintile,
    second_bpm,
    prev2_bpm,
    rows_in_artist_id,
    seq_no,
    second_second_bpm,
    LENGTH(TRIM(title)) AS title_len
FROM qualified
WHERE duration_sec > 142
ORDER BY bpm_quintile DESC, second_second_bpm DESC
