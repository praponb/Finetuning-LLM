WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        COALESCE(capacity, 0) AS capacity_filled,
        DATEDIFF(DATE '2025-06-30', opened_on) AS days_since_opened_on
    FROM warehouses
    WHERE LOWER(wh_code) LIKE '%kk%'
      AND opened_on IS NOT NULL
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.qty > 124
        )
),
totals AS (
    -- Group-level totals
    SELECT
        city,
        YEAR(opened_on) AS opened_on_year,
        SUM(CASE WHEN city IS NOT NULL THEN 1 ELSE 0 END) AS n_city_is_not_null,
        COUNT(*) FILTER (WHERE city IN ('Kuala Lumpur', 'Bangkok')) AS n_city_in_kuala_lumpur_bangkok,
        SUM(CASE WHEN city = 'Kuala Lumpur' THEN 1 ELSE 0 END) AS n_city_kuala_lumpur,
        SUM(days_since_opened_on) FILTER (WHERE wh_code IS NOT NULL) AS days_since_opened_on_wh_code_is_not_null
    FROM with_activity
    WHERE capacity_filled < 13350
    GROUP BY city, YEAR(opened_on)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE n_city_kuala_lumpur > (SELECT AVG(n_city_kuala_lumpur) FROM totals)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        city,
        opened_on_year,
        n_city_is_not_null,
        n_city_in_kuala_lumpur_bangkok,
        n_city_kuala_lumpur,
        days_since_opened_on_wh_code_is_not_null,
        ROUND(AVG(n_city_kuala_lumpur) OVER (PARTITION BY city), 2) AS city_avg_n_city_kuala_lumpur,
        n_city_kuala_lumpur - LAG(n_city_kuala_lumpur) OVER (PARTITION BY city ORDER BY n_city_in_kuala_lumpur_bangkok DESC, city, opened_on_year) AS n_city_kuala_lumpur_change,
        LAST_VALUE(opened_on_year) IGNORE NULLS OVER (PARTITION BY city ORDER BY n_city_in_kuala_lumpur_bangkok DESC, city, opened_on_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_on_year,
        ROUND(CUME_DIST() OVER (PARTITION BY city ORDER BY n_city_in_kuala_lumpur_bangkok DESC), 3) AS n_city_in_kuala_lumpur_bangkok_cume_dist,
        LAG(n_city_kuala_lumpur) OVER (ORDER BY n_city_is_not_null NULLS LAST, city, opened_on_year) AS prev_n_city_kuala_lumpur
    FROM without_matches
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM windowed
    WHERE opened_on_year < (SELECT MAX(opened_on_year) FROM windowed)
)
-- Native SQL UNPIVOT (wide to long)
SELECT city, opened_on_year, field, value
FROM (
    SELECT city, opened_on_year, prev_n_city_kuala_lumpur, n_city_kuala_lumpur, last_known_opened_on_year
    FROM filtered
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (prev_n_city_kuala_lumpur AS m_prev_n_city_kuala_lumpur, n_city_kuala_lumpur AS n_city_kuala_lumpur_v, last_known_opened_on_year AS last_known_opened_on_year_v)
) unp
ORDER BY city, opened_on_year, field
