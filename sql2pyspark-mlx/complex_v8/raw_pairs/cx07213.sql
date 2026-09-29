WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT *
    FROM tracks
    WHERE bpm < 155
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY is_explicit), 2) AS bpm_vs_is_explicit_avg,
        MAX(duration_sec) OVER (PARTITION BY is_explicit) AS is_explicit_max_duration_sec
    FROM base_tracks
),
artist_id_stats AS (
    -- Avg bpm_vs_is_explicit_avg per artist_id
    SELECT artist_id, AVG(bpm_vs_is_explicit_avg) AS avg_bpm_vs_is_explicit_avg
    FROM sequenced
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_bpm_vs_is_explicit_avg,
        ROUND(s.bpm_vs_is_explicit_avg - g.avg_bpm_vs_is_explicit_avg, 2) AS bpm_vs_is_explicit_avg_minus_avg
    FROM sequenced s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm_vs_is_explicit_avg > g.avg_bpm_vs_is_explicit_avg
),
artist_id_stats_2 AS (
    -- Median bpm per artist_id
    SELECT artist_id, PERCENTILE_APPROX(bpm, 0.5) AS median_bpm
    FROM outliers
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_bpm, ROUND(s.bpm - g.median_bpm, 2) AS bpm_minus_median
    FROM outliers s
    JOIN artist_id_stats_2 g ON g.artist_id = s.artist_id
    WHERE s.bpm >= g.median_bpm
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.moods,
        s.bpm_vs_is_explicit_avg,
        s.avg_bpm_vs_is_explicit_avg,
        s.bpm_vs_is_explicit_avg_minus_avg,
        s.bpm_minus_median,
        a.country AS artist_country,
        a.artist_name
    FROM vs_group s
    JOIN artists a ON a.id = s.artist_id
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        bpm_vs_is_explicit_avg,
        avg_bpm_vs_is_explicit_avg,
        bpm_vs_is_explicit_avg_minus_avg,
        bpm_minus_median,
        artist_name,
        COALESCE(bpm_minus_median, 0.0) AS bpm_minus_median_filled
    FROM combined_details
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.moods,
        s.bpm_vs_is_explicit_avg,
        s.avg_bpm_vs_is_explicit_avg,
        s.bpm_vs_is_explicit_avg_minus_avg,
        s.bpm_minus_median,
        s.artist_name,
        s.bpm_minus_median_filled,
        a.genre AS artist_genre
    FROM labeled s
    JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    artist_id,
    title,
    moods,
    bpm_vs_is_explicit_avg,
    avg_bpm_vs_is_explicit_avg,
    bpm_minus_median,
    artist_name,
    bpm_minus_median_filled
FROM with_parent_artist
ORDER BY avg_bpm_vs_is_explicit_avg, bpm_minus_median_filled DESC, id
LIMIT 5
