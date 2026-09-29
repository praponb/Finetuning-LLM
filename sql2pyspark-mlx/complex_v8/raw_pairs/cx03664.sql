WITH filtered AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        CONCAT(city, '#', CAST(id AS STRING)) AS city_ref
    FROM warehouses
    WHERE city <> 'Singapore'
      AND opened_on >= DATE '2024-04-13'
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        city_ref,
        SUBSTRING(TRIM(wh_code), 1, 3) AS wh_code_prefix,
        COALESCE(capacity, 0) AS capacity_filled,
        CONCAT_WS('-', wh_code, city_ref) AS wh_code_city_ref_label
    FROM filtered
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN city IN ('Kuala Lumpur', 'Bangkok') THEN 'group_a' WHEN city = 'Singapore' THEN 'group_b' ELSE 'other' END AS city_group,
        capacity + capacity_filled AS capacity_plus_capacity_filled
    FROM annotated
    WHERE capacity_filled > 3850
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        city,
        capacity,
        city_ref,
        wh_code_prefix,
        capacity_filled,
        city_group,
        COUNT(*) OVER (PARTITION BY city_group) AS rows_in_city_group,
        NTILE(5) OVER w_city_group_city_by_capacity AS capacity_filled_quintile,
        capacity - LAG(capacity) OVER (PARTITION BY city_group, city ORDER BY capacity_filled, id) AS capacity_change,
        DENSE_RANK() OVER (PARTITION BY city_group, city ORDER BY capacity_filled, id) AS capacity_filled_dense_rank,
        ROUND(capacity_filled - AVG(capacity_filled) OVER (PARTITION BY city_group), 2) AS capacity_filled_vs_city_group_avg,
        ROUND(AVG(capacity_filled) OVER (PARTITION BY city_group, city ORDER BY capacity_filled DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS capacity_filled_avg_4
    FROM derived
    WINDOW w_city_group_city_by_capacity AS (PARTITION BY city_group, city ORDER BY capacity_filled, id)
)
SELECT
    capacity_filled_quintile,
    city_group,
    COUNT(DISTINCT city_group) AS distinct_city_groups,
    COUNT(*) AS row_count,
    SUM(capacity_filled_avg_4) AS total_capacity_filled_avg_4
FROM ranked
GROUP BY CUBE(capacity_filled_quintile, city_group)
ORDER BY capacity_filled_quintile NULLS LAST, city_group NULLS LAST
