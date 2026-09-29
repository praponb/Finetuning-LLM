WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods,
        CASE WHEN duration_sec >= 357 THEN 'A' WHEN duration_sec >= 213 THEN 'B' ELSE 'C' END AS duration_sec_tier,
        DATE_ADD(release_date, 30) AS release_date_plus_30d
    FROM tracks
    WHERE (bpm >= 104 OR is_explicit = TRUE)
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.moods,
        s.duration_sec_tier,
        s.release_date_plus_30d,
        a.is_verified AS artist_is_verified
    FROM base_tracks s
    JOIN artists a ON a.id = s.artist_id
)
SELECT
    artist_is_verified,
    is_explicit,
    artist_id,
    COUNT(*) AS row_count,
    COUNT(DISTINCT duration_sec_tier) AS distinct_duration_sec_tiers,
    COUNT(*) FILTER (WHERE LOWER(title) LIKE '%ha%') AS n_lower_title_like_ha
FROM enriched_artist
GROUP BY ROLLUP(artist_is_verified, is_explicit, artist_id)
ORDER BY artist_is_verified NULLS LAST, is_explicit NULLS LAST, artist_id NULLS LAST
