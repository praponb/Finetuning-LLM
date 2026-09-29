WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE (duration_sec <= 309 OR bpm < 163)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN bpm >= 155 THEN 'large' WHEN bpm >= 100 THEN 'medium' ELSE 'small' END AS bpm_tier
    FROM valid_tracks
    WHERE bpm >= 94
),
derived AS (
    -- Derive helper columns
    SELECT *, UPPER(TRIM(bpm_tier)) AS bpm_tier_clean
    FROM annotated
),
with_activity AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.is_verified = TRUE
        )
)
SELECT
    COALESCE(bpm_tier, 'ALL') AS bpm_tier,
    artist_id,
    PERCENTILE_APPROX(bpm, 0.5) AS median_bpm
FROM with_activity
GROUP BY ROLLUP(bpm_tier, artist_id)
