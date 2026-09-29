WITH recent_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        is_active,
        LENGTH(TRIM(sku_code)) AS sku_code_len
    FROM skus
    WHERE (weight_kg >= 3.7 OR unit_cost <= 320.4)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_sku_code_len_by_sku_code_len AS sku_code_len_dense_rank,
        MAX(sku_code_len) OVER (PARTITION BY sku_code_len ORDER BY sku_code_len DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sku_code_len_max_3,
        ROUND(CUME_DIST() OVER w_sku_code_len_by_sku_code_len_3, 3) AS sku_code_len_cume_dist,
        LAST_VALUE(sku_code_len) IGNORE NULLS OVER (PARTITION BY sku_code_len, is_active ORDER BY sku_code_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sku_code_len,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_code_len, is_active ORDER BY sku_code_len ASC), 3) AS sku_code_len_percent_rank,
        MAX(sku_code_len) OVER (PARTITION BY sku_code_len) AS sku_code_len_max_sku_code_len
    FROM recent_skus
    WINDOW w_sku_code_len_by_sku_code_len AS (PARTITION BY sku_code_len ORDER BY sku_code_len DESC), w_sku_code_len_by_sku_code_len_3 AS (PARTITION BY sku_code_len ORDER BY sku_code_len DESC, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        sku_code,
        is_active,
        sku_code_len,
        sku_code_len_dense_rank,
        sku_code_len_max_3,
        sku_code_len_max_sku_code_len,
        ROUND(sku_code_len / NULLIF(last_known_sku_code_len, 0), 3) AS sku_code_len_per_last_known_sku_code_len,
        UPPER(TRIM(category)) AS category_clean
    FROM ranked
)
SELECT
    id,
    is_active,
    sku_code_len,
    sku_code_len_per_last_known_sku_code_len
FROM tagged
WHERE sku_code_len_per_last_known_sku_code_len <= 6.5
ORDER BY id DESC
