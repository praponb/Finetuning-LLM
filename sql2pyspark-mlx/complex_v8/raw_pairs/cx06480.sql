WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm + duration_sec AS bpm_plus_duration_sec,
        CASE WHEN duration_sec >= 312 THEN 'large' WHEN duration_sec >= 164 THEN 'medium' ELSE 'small' END AS duration_sec_tier,
        YEAR(release_date) AS release_date_year
    FROM tracks
    WHERE release_date IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(duration_sec_tier), ' ', '_')) AS duration_sec_tier_slug,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        CASE WHEN release_date_year >= 2026 THEN 'large' WHEN release_date_year >= 2025 THEN 'medium' ELSE 'small' END AS release_date_year_tier
    FROM filtered_tracks
)
-- Subtotals with GROUPING SETS
SELECT
    is_explicit,
    COALESCE(release_date_year_tier, 'ALL') AS release_date_year_tier,
    GROUPING(is_explicit) AS is_total_is_explicit,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count
FROM derived
GROUP BY GROUPING SETS ((is_explicit, release_date_year_tier), (is_explicit))
