WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
        CASE WHEN bpm >= 155 THEN 'large' WHEN bpm >= 94 THEN 'medium' ELSE 'small' END AS bpm_tier,
        COALESCE(bpm, 0) AS bpm_filled
    FROM tracks
    WHERE artist_id >= 8
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.moods_text,
        s.bpm_tier,
        s.bpm_filled,
        a.country AS artist_country
    FROM cleaned s
    LEFT JOIN artists a ON a.id = s.artist_id
),
unique_rows AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        moods_text,
        bpm_tier,
        bpm_filled,
        artist_country
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec, id) AS recency_rank
        FROM enriched_artist
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with ROLLUP
SELECT
    artist_id,
    COALESCE(artist_country, 'ALL') AS artist_country,
    ROUND(SUM(duration_sec) / NULLIF(SUM(bpm_filled), 0), 3) AS duration_sec_to_bpm_filled_ratio
FROM unique_rows
GROUP BY ROLLUP(artist_id, artist_country)
ORDER BY artist_id NULLS LAST, artist_country NULLS LAST
