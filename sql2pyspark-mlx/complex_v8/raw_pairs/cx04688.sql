WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'workout')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(duration_sec) OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_max_4,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg
    FROM cleaned
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.duration_sec_max_4,
        s.bpm_vs_artist_id_avg,
        a.is_verified AS artist_is_verified
    FROM windowed s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.is_verified = TRUE
)
-- Subtotals with GROUPING SETS
SELECT
    artist_is_verified,
    artist_id,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(title)) AS title_list,
    ROUND(SUM(bpm_vs_artist_id_avg), 2) AS total_bpm_vs_artist_id_avg,
    PERCENTILE_APPROX(bpm, 0.5) AS median_bpm
FROM joined
GROUP BY GROUPING SETS ((artist_is_verified, artist_id), (artist_is_verified))
ORDER BY artist_is_verified NULLS LAST, artist_id NULLS LAST
