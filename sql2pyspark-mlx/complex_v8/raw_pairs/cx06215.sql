WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        ARRAY_CONTAINS(moods, 'workout') AS has_workout,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        ARRAY_CONTAINS(moods, 'party') AS has_party
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'workout')
),
grouped AS (
    -- Group-level totals
    SELECT
        has_party,
        MIN(duration_sec_filled) AS min_duration_sec_filled,
        COUNT(*) FILTER (WHERE is_explicit = FALSE) AS n_is_explicit_false,
        MIN(release_date) AS first_release_date,
        SUM(CASE WHEN is_explicit = TRUE THEN bpm ELSE 0 END) AS bpm_is_explicit_true,
        COUNT(*) AS n_rows
    FROM recent_tracks
    WHERE artist_id <= 9
    GROUP BY has_party
    HAVING COUNT(*) >= 2
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(bpm_is_explicit_true) OVER (PARTITION BY has_party) AS has_party_sum_bpm_is_explicit_true,
        DENSE_RANK() OVER (PARTITION BY has_party ORDER BY min_duration_sec_filled, has_party) AS min_duration_sec_filled_dense_rank,
        MIN(n_is_explicit_false) OVER (PARTITION BY has_party) AS has_party_min_n_is_explicit_false,
        MIN(min_duration_sec_filled) OVER (PARTITION BY has_party) AS has_party_min_min_duration_sec_filled
    FROM grouped
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE has_party_min_n_is_explicit_false > (SELECT AVG(has_party_min_n_is_explicit_false) FROM metrics)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        IF(has_party, 'yes', 'no') AS has_party_label,
        CASE WHEN n_rows >= 10 THEN 'large' WHEN n_rows >= 5 THEN 'medium' ELSE 'small' END AS n_rows_tier,
        IF(has_party, 'yes', 'no') AS has_party_label_2
    FROM without_matches
    WHERE bpm_is_explicit_true >= 180
)
SELECT has_party, measure, metric_value
FROM (
    SELECT has_party, n_rows, has_party_min_min_duration_sec_filled, min_duration_sec_filled_dense_rank, n_is_explicit_false
    FROM labeled
) src
UNPIVOT (
    metric_value FOR measure IN (n_rows, has_party_min_min_duration_sec_filled, min_duration_sec_filled_dense_rank, n_is_explicit_false)
) unp
