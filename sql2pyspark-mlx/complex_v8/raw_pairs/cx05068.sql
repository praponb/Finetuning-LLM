WITH base_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CASE WHEN capacity >= 11506 THEN 'HIGH' WHEN capacity >= 4912 THEN 'MEDIUM' ELSE 'LOW' END AS capacity_tier,
        DATE_FORMAT(opened_on, 'yyyy-MM-dd') AS opened_on_label
    FROM warehouses
    WHERE LOWER(wh_code) LIKE '%k1%'
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month,
        SUM(CASE WHEN LOWER(wh_code) LIKE '%l1%' THEN 1 ELSE 0 END) AS n_lower_wh_code_like_l1,
        SUM(capacity) FILTER (WHERE city IN ('Singapore', 'Kuala Lumpur')) AS capacity_city_in_singapore_kuala_lump,
        SUM(CASE WHEN city IS NOT NULL THEN 1 ELSE 0 END) AS n_city_is_not_null,
        SUM(CASE WHEN UPPER(TRIM(wh_code)) LIKE 'C%' THEN 1 ELSE 0 END) AS n_upper_trim_wh_code_like_c,
        MAX(capacity) AS max_capacity,
        SUM(capacity) AS total_capacity
    FROM base_warehouses
    WHERE LOWER(wh_code) LIKE '%nx%'
    GROUP BY DATE_TRUNC('MONTH', opened_on)
    HAVING MAX(capacity) > 4912
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE total_capacity > (SELECT AVG(total_capacity) FROM rollup_base)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(n_upper_trim_wh_code_like_c / NULLIF(SUM(n_upper_trim_wh_code_like_c) OVER (PARTITION BY n_lower_wh_code_like_l1, n_upper_trim_wh_code_like_c), 0), 4) AS n_upper_trim_wh_code_like_c_share_of_n_lower_wh_,
        COUNT(*) OVER (PARTITION BY n_lower_wh_code_like_l1, n_upper_trim_wh_code_like_c) AS rows_in_n_lower_wh_code_like_l1_n_upper_trim_wh_,
        ROUND(PERCENT_RANK() OVER (ORDER BY max_capacity, opened_on_month), 3) AS max_capacity_percent_rank
    FROM filtered
)
SELECT
    opened_on_month,
    capacity_city_in_singapore_kuala_lump,
    n_city_is_not_null,
    n_upper_trim_wh_code_like_c,
    max_capacity,
    total_capacity,
    rows_in_n_lower_wh_code_like_l1_n_upper_trim_wh_,
    max_capacity_percent_rank,
    DATE_FORMAT(opened_on_month, 'yyyy-MM-dd HH:00') AS opened_on_month_label
FROM metrics
ORDER BY opened_on_month NULLS LAST, max_capacity_percent_rank DESC NULLS FIRST
