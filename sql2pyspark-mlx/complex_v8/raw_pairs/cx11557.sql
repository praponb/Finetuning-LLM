WITH valid_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        CASE WHEN capacity >= 11506 THEN 'large' WHEN capacity >= 11005 THEN 'medium' ELSE 'small' END AS capacity_tier,
        DATE_FORMAT(opened_on, 'yyyy-MM-dd') AS opened_on_label,
        ADD_MONTHS(opened_on, 1) AS opened_on_plus_1m
    FROM warehouses
),
per_group AS (
    -- Group-level totals
    SELECT
        city,
        COUNT(*) AS n_rows,
        MAX(capacity) AS max_capacity,
        COUNT(DISTINCT capacity_tier) AS distinct_capacity_tiers,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(wh_code)) LIKE 'B%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_wh_code_like_b
    FROM valid_warehouses
    WHERE capacity_tier NOT IN ('medium', 'large')
    GROUP BY city
),
tagged AS (
    -- Add calculated columns
    SELECT
        city,
        n_rows,
        max_capacity,
        distinct_capacity_tiers,
        pct_upper_trim_wh_code_like_b,
        LOWER(REGEXP_REPLACE(TRIM(city), ' ', '_')) AS city_slug
    FROM per_group
)
SELECT city, measure, amount
FROM (
    SELECT city, distinct_capacity_tiers, max_capacity, n_rows
    FROM tagged
) src
UNPIVOT (
    amount FOR measure IN (distinct_capacity_tiers AS m_distinct_capacity_tiers, max_capacity AS max_capacity_v, n_rows AS n_rows_v)
) unp
