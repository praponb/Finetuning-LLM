WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        DATE_FORMAT(opened_on, 'MMM yyyy') AS opened_on_label,
        DATE_FORMAT(opened_on, 'MMM yyyy') AS opened_on_label_2,
        CONCAT_WS(': ', city, wh_code) AS city_wh_code_label
    FROM warehouses
    WHERE city IN ('Kuala Lumpur', 'Bangkok')
),
totals AS (
    -- Group-level totals
    SELECT
        city,
        DATE_FORMAT(opened_on, 'yyyy-MM') AS opened_on_ym,
        COUNT(*) FILTER (WHERE city NOT IN ('Kuala Lumpur', 'Singapore')) AS n_city_not_in_kuala_lumpur_sin,
        MIN(opened_on) AS first_opened_on,
        COUNT(*) FILTER (WHERE LOWER(wh_code) LIKE '%bk%') AS n_lower_wh_code_like_bk,
        SUM(capacity) AS total_capacity
    FROM filtered
    GROUP BY city, DATE_FORMAT(opened_on, 'yyyy-MM')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(total_capacity) OVER (PARTITION BY n_city_not_in_kuala_lumpur_sin ORDER BY total_capacity DESC, city, opened_on_ym) AS next_total_capacity,
        n_lower_wh_code_like_bk - LAG(n_lower_wh_code_like_bk) OVER (PARTITION BY n_city_not_in_kuala_lumpur_sin ORDER BY total_capacity DESC, city, opened_on_ym) AS n_lower_wh_code_like_bk_change,
        MIN(n_lower_wh_code_like_bk) OVER (PARTITION BY n_lower_wh_code_like_bk) AS n_lower_wh_code_like_bk_min_n_lower_wh_code_like
    FROM totals
)
SELECT
    n_city_not_in_kuala_lumpur_sin,
    n_lower_wh_code_like_bk,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT city) AS distinct_citys
FROM metrics
GROUP BY GROUPING SETS ((n_city_not_in_kuala_lumpur_sin, n_lower_wh_code_like_bk), (n_city_not_in_kuala_lumpur_sin))
ORDER BY n_city_not_in_kuala_lumpur_sin NULLS LAST, n_lower_wh_code_like_bk NULLS LAST
