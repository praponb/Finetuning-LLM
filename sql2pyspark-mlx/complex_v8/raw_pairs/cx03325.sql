WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        ABS(duration_sec - 209) AS duration_sec_distance,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND duration_sec >= 209
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        title_prefix,
        ABS(duration_sec - 309) AS duration_sec_distance_2,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM filtered_tracks
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT s.*, a.artist_name, a.is_verified AS artist_is_verified
    FROM labeled s
    JOIN artists a ON a.id = s.artist_id AND NOT a.is_verified
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        is_explicit_label,
        artist_is_verified,
        ROUND(100 * COUNT(CASE WHEN artist_is_verified = TRUE THEN 1 END) / COUNT(*), 1) AS pct_artist_is_verified_true,
        COUNT(*) AS cnt,
        COUNT(DISTINCT is_explicit_label) AS distinct_is_explicit_labels,
        ROUND(AVG(duration_sec_distance_2), 2) AS avg_duration_sec_distance_2,
        ROUND(SUM(duration_sec_distance_2) / NULLIF(SUM(duration_sec), 0), 3) AS duration_sec_distance_2_to_duration_sec_ratio,
        MAX(duration_sec_distance_2) AS max_duration_sec_distance_2
    FROM with_parent_artist
    GROUP BY 1, 2
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM rollup_base
    WHERE cnt > (SELECT MIN(cnt) FROM rollup_base)
)
-- Native SQL UNPIVOT (wide to long)
SELECT is_explicit_label, artist_is_verified, measure, amount
FROM (
    SELECT is_explicit_label, artist_is_verified, max_duration_sec_distance_2, cnt, distinct_is_explicit_labels
    FROM filtered
) src
UNPIVOT (
    amount FOR measure IN (max_duration_sec_distance_2, cnt, distinct_is_explicit_labels)
) unp
