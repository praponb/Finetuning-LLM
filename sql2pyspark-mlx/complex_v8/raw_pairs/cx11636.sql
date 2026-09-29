WITH valid AS (
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
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN bpm >= 142 THEN 'large' WHEN bpm >= 89 THEN 'medium' ELSE 'small' END AS bpm_tier
    FROM valid
),
summary AS (
    -- Group-level totals
    SELECT
        bpm_tier,
        is_explicit,
        MAX(duration_sec) AS max_duration_sec,
        PERCENTILE_APPROX(bpm, 0.5) AS median_bpm,
        MIN(bpm) AS min_bpm,
        COUNT(*) AS cnt
    FROM tagged
    GROUP BY bpm_tier, is_explicit
)
SELECT *
FROM (
    SELECT cnt, is_explicit, bpm_tier, median_bpm
    FROM summary
) src
PIVOT (
    MAX(median_bpm) AS max_v, SUM(median_bpm) AS sum_v
    FOR bpm_tier IN ('large', 'medium')
)
ORDER BY cnt, is_explicit
