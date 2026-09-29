WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        MONTH(release_date) AS release_date_month
    FROM tracks
    WHERE duration_sec < 352
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        release_date_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date_month DESC, id DESC) AS recency_rank
        FROM valid_tracks
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    release_date_month,
    artist_id,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(bpm, 0.5) AS median_bpm,
    ROUND(STDDEV(release_date_month), 2) AS stddev_release_date_month
FROM deduped
GROUP BY GROUPING SETS ((release_date_month, artist_id), (release_date_month), (artist_id))
ORDER BY release_date_month NULLS LAST, artist_id NULLS LAST
