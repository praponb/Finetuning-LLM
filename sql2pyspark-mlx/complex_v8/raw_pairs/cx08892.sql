WITH valid_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        category,
        unit_cost,
        labels,
        is_active,
        LOWER(REGEXP_REPLACE(TRIM(sku_code), ' ', '_')) AS sku_code_slug,
        FLOOR(unit_cost / 50) * 50 AS unit_cost_bucket
    FROM skus
    WHERE weight_kg >= 6.4
      AND category IN ('Toys', 'Apparel', 'Electronics')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY is_active ORDER BY unit_cost DESC, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY unit_cost_bucket NULLS LAST, id) AS seq_no_2,
        FIRST_VALUE(unit_cost) OVER (PARTITION BY is_active, category ORDER BY unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_cost,
        ROW_NUMBER() OVER w_category_by_unit_cost_bucket AS seq_no_3,
        ROUND(unit_cost_bucket - AVG(unit_cost_bucket) OVER w_is_active_category, 2) AS unit_cost_bucket_vs_is_active_category_avg,
        SUM(unit_cost) OVER (PARTITION BY is_active) AS is_active_sum_unit_cost,
        MAX(unit_cost) OVER w_category_is_active AS category_is_active_max_unit_cost,
        COUNT(*) OVER (PARTITION BY category, is_active) AS rows_in_category_is_active
    FROM valid_skus
    WINDOW w_category_by_unit_cost_bucket AS (PARTITION BY category ORDER BY unit_cost_bucket, id), w_is_active_category AS (PARTITION BY is_active, category), w_category_is_active AS (PARTITION BY category, is_active)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(seq_no_2 / NULLIF(SUM(seq_no_2) OVER (PARTITION BY seq_no_3), 0), 4) AS seq_no_2_share_of_seq_no_3,
        LEAD(rows_in_category_is_active) OVER (PARTITION BY seq_no ORDER BY unit_cost_bucket, id) AS next_rows_in_category_is_active,
        COUNT(*) OVER w_seq_no_3 AS rows_in_seq_no_3,
        RANK() OVER (PARTITION BY seq_no_3, category ORDER BY unit_cost DESC) AS unit_cost_rank
    FROM with_windows
    WINDOW w_seq_no_3 AS (PARTITION BY seq_no_3)
)
SELECT
    id,
    category,
    unit_cost,
    labels,
    is_active,
    sku_code_slug,
    seq_no,
    seq_no_2,
    first_unit_cost,
    seq_no_3,
    unit_cost_bucket_vs_is_active_category_avg,
    is_active_sum_unit_cost,
    category_is_active_max_unit_cost,
    rows_in_category_is_active,
    seq_no_2_share_of_seq_no_3,
    rows_in_seq_no_3,
    unit_cost_rank,
    LOWER(REGEXP_REPLACE(TRIM(sku_code_slug), ' ', '_')) AS sku_code_slug_slug,
    FLOOR(rows_in_seq_no_3 / 5) * 5 AS rows_in_seq_no_3_bucket
FROM sequenced
ORDER BY rows_in_seq_no_3_bucket, category_is_active_max_unit_cost DESC NULLS FIRST
