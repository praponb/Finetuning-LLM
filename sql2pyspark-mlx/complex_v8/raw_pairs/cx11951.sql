WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        LAST_DAY(release_date) AS release_date_month_end
    FROM tracks
    WHERE bpm >= 100
),
grouped AS (
    -- Group-level totals
    SELECT
        is_explicit,
        ROUND(AVG(bpm), 2) AS avg_bpm,
        PERCENTILE_APPROX(is_explicit_flag, 0.5) AS median_is_explicit_flag,
        MAX(is_explicit_flag) AS max_is_explicit_flag,
        MAX(release_date_month_end) AS last_release_date_month_end,
        ROUND(AVG(is_explicit_flag), 2) AS avg_is_explicit_flag
    FROM cleaned_tracks
    WHERE bpm >= 100
    GROUP BY is_explicit
    HAVING AVG(is_explicit_flag) > 0
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE max_is_explicit_flag < (SELECT MAX(max_is_explicit_flag) FROM grouped)
),
scored AS (
    -- Add calculated columns
    SELECT
        is_explicit,
        max_is_explicit_flag,
        last_release_date_month_end,
        avg_is_explicit_flag,
        ROUND(avg_is_explicit_flag * 1.5, 2) AS avg_is_explicit_flag_adj
    FROM filtered
    WHERE max_is_explicit_flag >= 0
)
SELECT
    is_explicit,
    max_is_explicit_flag,
    last_release_date_month_end,
    avg_is_explicit_flag_adj
FROM scored
WHERE avg_is_explicit_flag_adj <= 1.1
