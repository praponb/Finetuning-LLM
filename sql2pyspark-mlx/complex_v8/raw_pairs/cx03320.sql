WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE release_date < DATE '2025-03-29'
      AND is_explicit = TRUE
),
rollup_base AS (
    -- Group-level totals
    SELECT
        is_explicit,
        COUNT(*) AS cnt,
        MAX(release_date) AS last_release_date,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(duration_sec) AS total_duration_sec,
        ROUND(SUM(bpm_per_duration_sec) / NULLIF(SUM(duration_sec), 0), 3) AS bpm_per_duration_sec_to_duration_sec_ratio
    FROM scoped
    WHERE artist_id <= 3
    GROUP BY is_explicit
    HAVING COUNT(*) >= 2
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        GREATEST(distinct_ids, cnt) AS greatest_distinct_ids_cnt,
        GREATEST(distinct_ids, cnt) AS greatest_distinct_ids_cnt_2
    FROM rollup_base
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_is_explicit_label_is_explici AS greatest_distinct_ids_cnt_dense_rank,
        COUNT(*) OVER (PARTITION BY greatest_distinct_ids_cnt, is_explicit) AS rows_in_greatest_distinct_ids_cnt_is_explicit,
        SUM(distinct_ids) OVER (PARTITION BY greatest_distinct_ids_cnt ORDER BY bpm_per_duration_sec_to_duration_sec_ratio DESC, is_explicit ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_distinct_ids
    FROM tagged
    WINDOW w_is_explicit_label_is_explici AS (PARTITION BY is_explicit_label, is_explicit ORDER BY greatest_distinct_ids_cnt ASC)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LAST_DAY(last_release_date) AS last_release_date_month_end,
        LOWER(REGEXP_REPLACE(TRIM(is_explicit_label), ' ', '_')) AS is_explicit_label_slug,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label_2
    FROM ranked
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(is_explicit_label, '#', CAST(total_duration_sec AS STRING)) AS is_explicit_label_ref,
        CASE WHEN running_distinct_ids >= 75 THEN 'large' WHEN running_distinct_ids >= 36 THEN 'medium' ELSE 'small' END AS running_distinct_ids_tier,
        LOWER(REGEXP_REPLACE(TRIM(is_explicit_label_slug), ' ', '_')) AS is_explicit_label_slug_slug
    FROM enriched
)
SELECT
    is_explicit,
    cnt,
    last_release_date,
    distinct_ids,
    total_duration_sec,
    bpm_per_duration_sec_to_duration_sec_ratio,
    is_explicit_label,
    greatest_distinct_ids_cnt_2,
    rows_in_greatest_distinct_ids_cnt_is_explicit,
    running_distinct_ids,
    last_release_date_month_end,
    is_explicit_label_slug,
    is_explicit_label_2,
    is_explicit_label_ref,
    running_distinct_ids_tier,
    is_explicit_label_slug_slug,
    COALESCE(running_distinct_ids, 0) AS running_distinct_ids_filled,
    COALESCE(cnt, 0) AS cnt_filled
FROM enriched_2
ORDER BY is_explicit_label_slug DESC, is_explicit_label_2 DESC NULLS FIRST
