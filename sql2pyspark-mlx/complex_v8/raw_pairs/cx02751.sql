WITH scoped AS (
    -- Scope the tracks rows
    SELECT *
    FROM tracks
    WHERE bpm >= 76
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        ROUND(AVG(bpm) OVER (ORDER BY release_date NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_3,
        LAST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm
    FROM scoped
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        CASE WHEN duration_sec >= 352 THEN 'HIGH' WHEN duration_sec >= 213 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_tier
    FROM with_windows
    WHERE bpm_avg_3 >= 78.0
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(title, '#', CAST(duration_sec_bucket AS STRING)) AS title_ref,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        COALESCE(duration_sec_tier, 'UNKNOWN') AS duration_sec_tier_or_unknown
    FROM enriched
)
SELECT
    id,
    title,
    release_date,
    moods,
    last_known_bpm,
    title_ref,
    release_date_month,
    CONCAT_WS(' / ', duration_sec_tier, title_ref) AS duration_sec_tier_title_ref_label
FROM tagged
ORDER BY duration_sec_tier_title_ref_label DESC, id
LIMIT 20
