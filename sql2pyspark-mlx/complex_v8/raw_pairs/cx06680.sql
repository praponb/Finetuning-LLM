WITH prepared AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CASE WHEN city IN ('Bangkok', 'Singapore') THEN 'group_a' WHEN city = 'Kuala Lumpur' THEN 'group_b' ELSE 'other' END AS city_group,
        DATEDIFF(DATE '2025-12-31', opened_on) AS days_since_opened_on,
        COALESCE(capacity, 0) AS capacity_filled
    FROM warehouses
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.operator = 'op_dew'
              AND st.sku_id IS NOT NULL
        )
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month,
        city,
        SUM(capacity) FILTER (WHERE wh_code IS NOT NULL) AS capacity_wh_code_is_not_null,
        COUNT(*) FILTER (WHERE UPPER(TRIM(wh_code)) LIKE 'K%') AS n_upper_trim_wh_code_like_k,
        COUNT(*) FILTER (WHERE LOWER(wh_code) LIKE '%l1%') AS n_lower_wh_code_like_l1,
        ARRAY_SORT(COLLECT_SET(city)) AS city_list,
        ROUND(SUM(capacity_filled) / NULLIF(SUM(days_since_opened_on), 0), 3) AS capacity_filled_to_days_since_opened_on_ratio,
        COUNT(*) AS row_count
    FROM with_activity
    GROUP BY 1, 2
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        YEAR(opened_on_month) AS opened_on_month_year,
        CASE WHEN n_lower_wh_code_like_l1 >= 8 THEN 'large' WHEN n_lower_wh_code_like_l1 >= 4 THEN 'medium' ELSE 'small' END AS n_lower_wh_code_like_l1_tier
    FROM stats
),
exploded AS (
    -- One row per city_list_item
    SELECT
        s.opened_on_month,
        s.city,
        s.capacity_wh_code_is_not_null,
        s.n_upper_trim_wh_code_like_k,
        s.n_lower_wh_code_like_l1,
        s.capacity_filled_to_days_since_opened_on_ratio,
        s.row_count,
        s.opened_on_month_year,
        s.n_lower_wh_code_like_l1_tier,
        t.city_list_item
    FROM derived s
    LATERAL VIEW EXPLODE(s.city_list) t AS city_list_item
)
-- Native SQL UNPIVOT (wide to long)
SELECT opened_on_month, city, metric, amount
FROM (
    SELECT opened_on_month, city, n_upper_trim_wh_code_like_k, row_count, opened_on_month_year, n_lower_wh_code_like_l1
    FROM exploded
) src
UNPIVOT (
    amount FOR metric IN (n_upper_trim_wh_code_like_k AS m_n_upper_trim_wh_code_like_k, row_count AS m_row_count, opened_on_month_year AS m_opened_on_month_year, n_lower_wh_code_like_l1 AS m_n_lower_wh_code_like_l1)
) unp
