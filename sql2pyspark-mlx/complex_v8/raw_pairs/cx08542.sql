WITH filtered AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        labels,
        is_active,
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag,
        SIZE(labels) AS labels_count
    FROM skus
    WHERE LOWER(sku_code) LIKE '%-2%'
),
per_group AS (
    -- Group-level totals
    SELECT
        is_active_flag,
        labels_count,
        is_active,
        SUM(CASE WHEN UPPER(TRIM(sku_code)) LIKE 'A%' THEN labels_count ELSE 0 END) AS labels_count_upper_trim_sku_code_like_a,
        COUNT(*) AS row_count
    FROM filtered
    GROUP BY is_active_flag, labels_count, is_active
),
annotated AS (
    -- Derive helper columns
    SELECT
        is_active_flag,
        labels_count,
        is_active,
        labels_count_upper_trim_sku_code_like_a,
        row_count,
        ROUND(is_active_flag / NULLIF(row_count, 0), 3) AS is_active_flag_per_row_count,
        IF(is_active, 'yes', 'no') AS is_active_label
    FROM per_group
    WHERE labels_count >= 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(is_active_flag_per_row_count) IGNORE NULLS OVER (PARTITION BY is_active_label, is_active ORDER BY row_count DESC NULLS FIRST, is_active_flag, labels_count, is_active ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_active_flag_per_row_count,
        COUNT(*) OVER (PARTITION BY labels_count, is_active_label) AS rows_in_labels_count_is_active_label,
        NTILE(4) OVER (PARTITION BY is_active_label, is_active ORDER BY row_count DESC NULLS FIRST, is_active_flag, labels_count, is_active) AS row_count_quartile,
        ROUND(row_count - AVG(row_count) OVER (PARTITION BY labels_count, is_active_label), 2) AS row_count_vs_labels_count_is_active_label_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_active_label, is_active ORDER BY row_count DESC NULLS FIRST, is_active_flag, labels_count, is_active), 3) AS row_count_percent_rank,
        row_count - LAG(row_count) OVER (PARTITION BY is_active_label, is_active ORDER BY row_count DESC NULLS FIRST, is_active_flag, labels_count, is_active) AS row_count_change
    FROM annotated
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        is_active_flag,
        labels_count,
        is_active,
        row_count,
        is_active_label,
        row_count_quartile,
        row_count_vs_labels_count_is_active_label_avg,
        row_count_change,
        ROW_NUMBER() OVER w_is_active_row_count_quartile AS rn,
        ROW_NUMBER() OVER (ORDER BY row_count_vs_labels_count_is_active_label_avg ASC, is_active_flag, labels_count, is_active) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY is_active, row_count_quartile ORDER BY rows_in_labels_count_is_active_label) AS rows_in_labels_count_is_active_label_dense_rank
    FROM ranked
    WINDOW w_is_active_row_count_quartile AS (PARTITION BY is_active, row_count_quartile ORDER BY rows_in_labels_count_is_active_label, is_active_flag, labels_count, is_active)
),
scored AS (
    -- Label and score rows
    SELECT *, row_count_change - rn AS row_count_change_minus_rn
    FROM with_windows
)
-- Final projection
SELECT DISTINCT labels_count
FROM scored
WHERE row_count_change_minus_rn > 2
ORDER BY labels_count
