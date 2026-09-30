WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE (YEAR(release_date) = 2024 OR duration_sec > 209)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('|', title_slug, is_explicit_label) AS title_slug_is_explicit_label_label,
        CONCAT(title_slug, '#', CAST(bpm AS STRING)) AS title_slug_ref,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label_2
    FROM valid_tracks
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY is_explicit_label_2, is_explicit_label) AS rows_in_is_explicit_label_2_is_explicit_label,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm, id), 3) AS bpm_percent_rank
    FROM enriched
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT artist_id, is_explicit, is_explicit_label, duration_sec
    FROM with_trends
) src
PIVOT (
    SUM(duration_sec)
    FOR is_explicit_label IN ('yes', 'no')
)
