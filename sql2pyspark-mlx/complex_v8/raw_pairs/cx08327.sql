WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        ARRAY_CONTAINS(moods, 'party') AS has_party,
        UPPER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE release_date < DATE '2025-08-02'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        s.has_party,
        s.title_clean,
        a.is_verified AS artist_is_verified,
        a.artist_name
    FROM cleaned s
    JOIN artists a ON a.id = s.artist_id
),
artist_id_stats AS (
    -- Median bpm per artist_id
    SELECT artist_id, PERCENTILE_APPROX(bpm, 0.5) AS median_bpm
    FROM with_parent
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_bpm, ROUND(s.bpm - g.median_bpm, 2) AS bpm_minus_median
    FROM with_parent s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm >= g.median_bpm
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        bpm,
        has_party,
        artist_is_verified,
        artist_name,
        median_bpm,
        bpm_minus_median,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm_minus_median, id) AS bpm_minus_median_seq,
        LAG(bpm) OVER (PARTITION BY artist_id ORDER BY bpm_minus_median, id) AS prev_bpm,
        NTH_VALUE(median_bpm, 2) OVER (PARTITION BY artist_id ORDER BY bpm_minus_median, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_median_bpm
    FROM above_average
)
SELECT
    bpm_minus_median_seq,
    artist_is_verified,
    ROUND(AVG(prev_bpm), 2) AS avg_prev_bpm,
    ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list,
    ROUND(100 * COUNT(CASE WHEN artist_is_verified = TRUE THEN 1 END) / COUNT(*), 1) AS pct_artist_is_verified_true
FROM with_windows
GROUP BY GROUPING SETS ((bpm_minus_median_seq, artist_is_verified), (bpm_minus_median_seq), (artist_is_verified))
ORDER BY bpm_minus_median_seq NULLS LAST, artist_is_verified NULLS LAST
