WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE duration_sec <= 357
),
latest_per_key AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS row_num
        FROM scoped_tracks
    ) ranked
    WHERE row_num = 1
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN bpm >= 156 THEN 'A' WHEN bpm >= 100 THEN 'B' ELSE 'C' END AS bpm_tier,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        COALESCE(bpm, 0) AS bpm_filled
    FROM latest_per_key
)
-- Subtotals with GROUPING SETS
SELECT
    bpm_tier,
    is_explicit,
    GROUPING(bpm_tier) AS is_total_bpm_tier,
    ROUND(100 * COUNT(CASE WHEN NOT is_explicit THEN 1 END) / COUNT(*), 1) AS pct_not_is_explicit
FROM annotated
GROUP BY GROUPING SETS ((bpm_tier, is_explicit), (bpm_tier))
ORDER BY bpm_tier NULLS LAST, is_explicit NULLS LAST
