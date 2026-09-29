WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        SIZE(moods) AS moods_count,
        LENGTH(TRIM(title)) AS title_len,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 4
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.moods_count,
        s.bpm_per_duration_sec,
        a.is_verified AS artist_is_verified
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
),
artist_id_stats AS (
    -- Max moods_count per artist_id
    SELECT artist_id, MAX(moods_count) AS max_moods_count
    FROM combined_details
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_moods_count,
        ROUND(s.moods_count - g.max_moods_count, 2) AS moods_count_minus_max
    FROM combined_details s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.moods_count = g.max_moods_count
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        release_date,
        moods_count,
        bpm_per_duration_sec,
        artist_is_verified,
        max_moods_count,
        moods_count_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY moods_count_minus_max DESC, id DESC) AS row_num
        FROM above_average
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with CUBE
SELECT
    artist_is_verified,
    artist_id,
    GROUPING(artist_is_verified) AS is_total_artist_is_verified,
    PERCENTILE_APPROX(bpm_per_duration_sec, 0.5) AS median_bpm_per_duration_sec
FROM deduped
GROUP BY CUBE(artist_is_verified, artist_id)
ORDER BY artist_is_verified NULLS LAST, artist_id NULLS LAST
