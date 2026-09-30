WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE artist_id <= 2
      AND is_explicit = TRUE
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        LOWER(TRIM(title)) AS title_clean,
        UPPER(TRIM(title)) AS title_clean_2,
        CASE WHEN bpm >= 163 THEN 'HIGH' WHEN bpm >= 89 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier
    FROM cleaned
),
enriched AS (
    -- Derive helper columns
    SELECT *, UPPER(TRIM(bpm_tier)) AS bpm_tier_clean
    FROM scored
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        SUM(bpm) OVER (PARTITION BY bpm_tier, is_explicit) AS bpm_tier_is_explicit_sum_bpm,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec, id) AS duration_sec_change
    FROM enriched
)
-- Final projection
SELECT
    id,
    duration_sec,
    is_explicit,
    bpm,
    moods,
    title_clean,
    title_clean_2,
    bpm_tier,
    second_bpm,
    bpm_tier_is_explicit_sum_bpm,
    duration_sec_change,
    CASE WHEN duration_sec >= 352 THEN 'HIGH' WHEN duration_sec >= 165 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_tier
FROM sequenced
