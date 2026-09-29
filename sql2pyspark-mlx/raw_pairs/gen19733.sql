WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        ROUND(units * 0.9, 2) units_scaled,
        ROUND(units, 1) units_rounded
    FROM store_sales
    WHERE revenue BETWEEN 1800 AND 3300
),
with_dims AS (
    SELECT
        e.*,
        p.city,
        p.sqft,
        p.store_name
    FROM valid e
    LEFT JOIN stores p
        ON e.store_id = p.id
    WHERE MONTH(p.opened_date) IN (1, 10, 11)
),
scored AS (
    SELECT
        id,
        store_id,
        units,
        revenue,
        units_scaled,
        city,
        sqft,
        SUM(sqft) OVER (PARTITION BY city ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft,
        FIRST_VALUE(units_scaled, TRUE) OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_units_scaled
    FROM with_dims
),
scored_store_sales AS (
    SELECT
        id,
        revenue,
        city,
        sqft,
        first_known_units_scaled,
        LAG(running_sqft) OVER w_grp prev_running_sqft,
        LAG(first_known_units_scaled) OVER w_grp prev_first_known_units_scaled,
        ROW_NUMBER() OVER w_grp row_seq,
        LAG(revenue, 2) OVER (PARTITION BY city ORDER BY sqft, id) prev_revenue
    FROM scored
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY units, id)
)
SELECT
    id,
    city,
    first_known_units_scaled,
    prev_running_sqft,
    row_seq,
    prev_revenue,
    CASE WHEN city RLIKE '^(London|Tokyo)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS city_zone
FROM scored_store_sales
