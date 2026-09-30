WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        weight_kg,
        labels,
        is_active,
        LOWER(REGEXP_REPLACE(TRIM(category), ' ', '_')) AS category_slug,
        FLOOR(weight_kg / 5) * 5 AS weight_kg_bucket
    FROM skus
    WHERE category = 'Electronics'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        is_active,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list,
        COUNT(*) AS row_count
    FROM cleaned
    WHERE sku_code IS NOT NULL
    GROUP BY 1
    HAVING MAX(weight_kg_bucket) > 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(row_count) OVER (PARTITION BY is_active ORDER BY row_count, is_active ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS row_count_avg_7,
        COUNT(*) OVER (PARTITION BY row_count) AS rows_in_row_count,
        NTH_VALUE(row_count, 2) OVER (PARTITION BY row_count, is_active ORDER BY row_count DESC, is_active ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_row_count,
        ROUND(row_count / NULLIF(SUM(row_count) OVER (PARTITION BY row_count), 0), 4) AS row_count_share_of_row_count,
        ROW_NUMBER() OVER w_is_active_by_row_count AS seq_no
    FROM stats
    WINDOW w_is_active_by_row_count AS (PARTITION BY is_active ORDER BY row_count, is_active)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM ranked
    WHERE row_count > (SELECT AVG(row_count) FROM ranked)
),
flattened AS (
    -- One row per sku_code_list_item
    SELECT
        s.is_active,
        s.row_count,
        s.row_count_avg_7,
        s.rows_in_row_count,
        s.second_row_count,
        s.row_count_share_of_row_count,
        s.seq_no,
        t.sku_code_list_item
    FROM qualified s
    LATERAL VIEW OUTER EXPLODE(s.sku_code_list) t AS sku_code_list_item
),
annotated AS (
    -- Add calculated columns
    SELECT
        is_active,
        row_count,
        row_count_avg_7,
        rows_in_row_count,
        second_row_count,
        seq_no,
        sku_code_list_item,
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag,
        ROUND(second_row_count / NULLIF(rows_in_row_count, 0), 3) AS second_row_count_per_rows_in_row_count
    FROM flattened
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM annotated
    WHERE is_active_flag > (SELECT MIN(is_active_flag) FROM annotated)
)
SELECT
    is_active,
    row_count,
    row_count_avg_7,
    rows_in_row_count,
    second_row_count,
    seq_no,
    sku_code_list_item,
    is_active_flag,
    ROUND(row_count_avg_7 * 1.1, 2) AS row_count_avg_7_adj
FROM filtered
