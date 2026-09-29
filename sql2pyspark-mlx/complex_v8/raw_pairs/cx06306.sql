WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        CASE WHEN bpm >= 142 THEN 'large' WHEN bpm >= 94 THEN 'medium' ELSE 'small' END AS bpm_tier,
        MONTH(release_date) AS release_date_month
    FROM tracks
    WHERE (release_date IS NOT NULL OR duration_sec BETWEEN 144 AND 409)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        is_explicit_label,
        MAX(release_date_month) AS max_release_date_month,
        COUNT(DISTINCT is_explicit_label) AS distinct_is_explicit_labels,
        SUM(bpm) AS total_bpm,
        ROUND(SUM(bpm) / NULLIF(SUM(release_date_month), 0), 3) AS bpm_to_release_date_month_ratio,
        SUM(release_date_month) AS total_release_date_month,
        ROUND(SUM(duration_sec) / NULLIF(SUM(release_date_month), 0), 3) AS duration_sec_to_release_date_month_ratio
    FROM scoped_tracks
    GROUP BY is_explicit_label
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        is_explicit_label,
        distinct_is_explicit_labels,
        total_bpm,
        bpm_to_release_date_month_ratio,
        total_release_date_month,
        NTH_VALUE(total_bpm, 2) OVER (ORDER BY bpm_to_release_date_month_ratio DESC, is_explicit_label ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_bpm,
        COUNT(*) OVER (PARTITION BY is_explicit_label, distinct_is_explicit_labels) AS rows_in_is_explicit_label_distinct_is_explicit_l,
        ROW_NUMBER() OVER (ORDER BY bpm_to_release_date_month_ratio DESC, is_explicit_label) AS seq_no,
        ROUND(total_bpm - AVG(total_bpm) OVER w_is_explicit_label_distinct_i, 2) AS total_bpm_vs_is_explicit_label_distinct_is_expli,
        LEAD(total_release_date_month) OVER w_is_explicit_label_by_max_rel AS next_total_release_date_month
    FROM rollup_base
    WINDOW w_is_explicit_label_distinct_i AS (PARTITION BY is_explicit_label, distinct_is_explicit_labels), w_is_explicit_label_by_max_rel AS (PARTITION BY is_explicit_label ORDER BY max_release_date_month ASC, is_explicit_label)
)
-- Native SQL UNPIVOT (wide to long)
SELECT is_explicit_label, measure, reading
FROM (
    SELECT is_explicit_label, distinct_is_explicit_labels, next_total_release_date_month
    FROM sequenced
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (distinct_is_explicit_labels, next_total_release_date_month)
) unp
