WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM tracks
    WHERE (release_date < DATE '2025-08-02' OR duration_sec > 213)
),
with_parent_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM valid s
    JOIN artists a ON a.id = s.artist_id
),
artist_id_stats AS (
    -- Avg duration_sec per artist_id
    SELECT artist_id, AVG(duration_sec) AS avg_duration_sec
    FROM with_parent_artist
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_duration_sec,
        ROUND(s.duration_sec - g.avg_duration_sec, 2) AS duration_sec_minus_avg
    FROM with_parent_artist s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.duration_sec > g.avg_duration_sec
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(bpm, 0) AS bpm_filled,
        ROUND(avg_duration_sec * 1.5, 2) AS avg_duration_sec_adj,
        LEAST(artist_debut_year, bpm) AS least_artist_debut_year_bpm
    FROM vs_group
    WHERE SIZE(moods) >= 2
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        artist_is_verified,
        artist_debut_year,
        avg_duration_sec,
        duration_sec_minus_avg,
        bpm_filled,
        avg_duration_sec_adj,
        least_artist_debut_year_bpm
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY avg_duration_sec_adj DESC, id DESC) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    is_explicit,
    bpm,
    moods,
    artist_is_verified,
    avg_duration_sec,
    bpm_filled,
    avg_duration_sec_adj,
    LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
    ROUND(duration_sec_minus_avg * 0.9, 2) AS duration_sec_minus_avg_adj
FROM deduped
