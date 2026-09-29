WITH recent AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        labels,
        GREATEST(unit_cost, weight_kg) AS greatest_unit_cost_weight_kg
    FROM skus
    WHERE category = 'Beauty'
      AND sku_code IS NOT NULL
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
        )
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY weight_kg DESC, id) AS rn,
        COUNT(*) OVER (ORDER BY greatest_unit_cost_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(5) OVER (ORDER BY greatest_unit_cost_weight_kg, id) AS greatest_unit_cost_weight_kg_quintile
    FROM without_matches
)
-- Subtotals with GROUPING SETS
SELECT
    rn,
    greatest_unit_cost_weight_kg_quintile,
    GROUPING(rn) AS is_total_rn,
    ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(sku_code)) LIKE 'E%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_sku_code_like_e,
    COUNT(*) AS n_rows,
    PERCENTILE_APPROX(running_count, 0.5) AS median_running_count
FROM sequenced
GROUP BY GROUPING SETS ((rn, greatest_unit_cost_weight_kg_quintile), (rn), (greatest_unit_cost_weight_kg_quintile))
ORDER BY rn NULLS LAST, greatest_unit_cost_weight_kg_quintile NULLS LAST
